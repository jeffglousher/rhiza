use std::{path::Path, sync::Arc, time::Duration};

use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
use rhiza_core::{ConfigurationState, LogAnchor};
use rhiza_node::{
    durability::{finalize_successor_prestage_for_stop, inspect_successor_prestage},
    install_successor_recorder, NodeRuntime,
};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{Membership, RecorderFileStore, RecorderRpc, ThreeNodeConsensus};
use rhizadb::{
    CertifiedTailRecord, CertifiedTailResponse, CheckpointCoordinator, DurabilityMode,
    ExecutionProfile, HaPredecessor, HaRecorderTransport, HaServeConfig, HaStartupConfig,
    HaStartupMode, HaSuccessorPrestageConfig, NodeConfig, PeerConfig, StopInformation,
};

fn archive(root: &Path) -> ObjectArchiveStore {
    ObjectArchiveStore::new_checkpoint_for_single_process(
        ObjStore::new(ObjStoreConfig::Local {
            root: root.to_path_buf(),
        })
        .unwrap(),
        CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 1),
    )
}

fn node_config(data_dir: &Path) -> NodeConfig {
    let peers = ["node-1", "node-2", "node-3"]
        .into_iter()
        .enumerate()
        .map(|(index, node_id)| {
            PeerConfig::new(
                node_id,
                format!("http://127.0.0.1:{}", 9101 + index),
                format!("peer-token-{}", index + 1),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    NodeConfig::new(
        "cluster-a",
        "node-1",
        data_dir.to_path_buf(),
        1,
        1,
        peers,
        "client-token",
    )
    .unwrap()
    .with_execution_profile(ExecutionProfile::Sqlite)
    .unwrap()
}

fn recorder_clients(root: &Path) -> Vec<(String, Box<dyn RecorderRpc>)> {
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    membership
        .members()
        .iter()
        .map(|node_id| {
            let recorder = RecorderFileStore::new_with_membership(
                root.join(node_id),
                node_id.clone(),
                "rhiza:sql:cluster-a",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (node_id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>)
        })
        .collect()
}

fn successor_recorder_clients(
    root: &Path,
    predecessor: &Membership,
    successor: &Membership,
    stop: &StopInformation,
) -> Vec<(String, Box<dyn RecorderRpc>)> {
    successor
        .members()
        .iter()
        .map(|node_id| {
            let recorder = RecorderFileStore::new_with_membership(
                root.join(node_id),
                node_id.clone(),
                "rhiza:sql:cluster-a",
                1,
                1,
                predecessor.clone(),
            )
            .unwrap();
            install_successor_recorder(&recorder, 2, successor.clone(), stop).unwrap();
            (node_id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>)
        })
        .collect()
}

#[tokio::test]
async fn ha_bootstrap_rejects_nonfresh_local_state_before_opening_a_recorder() {
    let root = tempfile::tempdir().unwrap();
    let archive = archive(&root.path().join("archive"));
    archive.initialize_checkpoint().await.unwrap();
    let data_dir = root.path().join("node");
    std::fs::create_dir_all(data_dir.join("sqlite")).unwrap();
    std::fs::write(data_dir.join("sqlite/existing"), b"state").unwrap();

    let error = HaStartupConfig::new(
        node_config(&data_dir),
        archive,
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Bootstrap,
    )
    .prepare()
    .await
    .unwrap_err();

    assert!(error.to_string().contains("fresh local data directory"));
    assert!(!data_dir.join("recorder").exists());
}

#[tokio::test]
async fn ha_standard_startup_rejects_bound_stopped_successor_before_creating_local_state() {
    let root = tempfile::tempdir().unwrap();
    let archive = archive(&root.path().join("archive"));
    archive.initialize_checkpoint().await.unwrap();
    let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let successor = Membership::new(["node-4", "node-5", "node-6"]).unwrap();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
            recorder_clients(&root.path().join("source-recorders")),
        )
        .unwrap(),
    );
    let source =
        NodeRuntime::open(node_config(&root.path().join("source")), consensus, &[]).unwrap();
    let stop = source
        .stop_current_configuration_for_successor(&successor)
        .unwrap();
    assert!(source
        .consensus()
        .finish_pending_rpcs(Duration::from_secs(1)));
    let peers = successor
        .members()
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            PeerConfig::new(
                node_id,
                format!("http://127.0.0.1:{}", 9201 + index),
                format!("successor-peer-token-{}", index + 1),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let data_dir = root.path().join("successor");
    let successor_config = NodeConfig::new_with_configuration(
        "rhiza:sql:cluster-a",
        "node-4",
        data_dir.clone(),
        1,
        successor,
        source.configuration_state().unwrap(),
        peers,
        "successor-client-token",
    )
    .unwrap()
    .with_log_initial_configuration(ConfigurationState::active(1, predecessor.digest()))
    .with_predecessor_stop_entry(stop.entry);

    let error = HaStartupConfig::new(
        successor_config,
        archive,
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Bootstrap,
    )
    .prepare()
    .await
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("stopped configuration requires a predecessor"),
        "{error}"
    );
    assert!(!data_dir.exists());
}

#[tokio::test]
async fn successor_prestage_exposes_the_exact_seed_and_target_without_opening_a_runtime() {
    let root = tempfile::tempdir().unwrap();
    let archive = archive(&root.path().join("archive"));
    archive.initialize_checkpoint().await.unwrap();
    let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let successor = Membership::new(["node-4", "node-5", "node-6"]).unwrap();

    let prepared = HaSuccessorPrestageConfig::new(
        archive,
        root.path().join("prestage"),
        "node-4",
        ExecutionProfile::Sqlite,
        predecessor,
        successor.clone(),
        "tail-token",
    )
    .prepare()
    .await
    .unwrap();

    assert_eq!(prepared.identity().cluster_id(), "rhiza:sql:cluster-a");
    assert_eq!(prepared.identity().epoch(), 1);
    assert_eq!(prepared.identity().predecessor_config_id(), 1);
    assert_eq!(prepared.identity().predecessor_recovery_generation(), 1);
    assert_eq!(prepared.identity().target_config_id(), 2);
    assert_eq!(
        prepared.identity().target_membership_digest(),
        successor.digest()
    );
    assert_eq!(
        prepared.identity().seed_anchor(),
        LogAnchor::new(0, rhiza_core::LogHash::ZERO)
    );
    assert_eq!(
        prepared.tail_request(8).unwrap().from,
        prepared.identity().seed_anchor()
    );
    assert!(!root.path().join("prestage/recorder").exists());
}

#[tokio::test]
async fn finalized_successor_restart_adopts_exact_stop_and_opens_awaiting_activation() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjStore::new(ObjStoreConfig::Local {
        root: root.path().join("archive"),
    })
    .unwrap();
    let source_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store.clone(),
        CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 1),
    );
    source_archive.initialize_checkpoint().await.unwrap();
    let coordinator = CheckpointCoordinator::open(source_archive.clone(), DurabilityMode::Sync)
        .await
        .unwrap();
    let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let successor = Membership::new(["node-4", "node-5", "node-6"]).unwrap();
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
            recorder_clients(&root.path().join("source-recorders")),
        )
        .unwrap(),
    );
    let source =
        NodeRuntime::open(node_config(&root.path().join("source")), consensus, &[]).unwrap();
    source.write("seed-write", "key", "seed").unwrap();
    let seed = source.checkpoint_compact(&coordinator).await.unwrap();
    let target_data_dir = root.path().join("successor");
    let learner = HaSuccessorPrestageConfig::new(
        source_archive.clone(),
        root.path().join("prestage"),
        "node-4",
        ExecutionProfile::Sqlite,
        predecessor.clone(),
        successor.clone(),
        "tail-token",
    )
    .prepare()
    .await
    .unwrap();
    assert_eq!(learner.identity().seed_anchor(), *seed.compacted());
    let learner = learner.publish(&target_data_dir).unwrap();

    let stop = source
        .stop_current_configuration_for_successor(&successor)
        .unwrap();
    assert!(source
        .consensus()
        .finish_pending_rpcs(Duration::from_secs(1)));
    let request = learner.tail_request(8).unwrap();
    let stop_anchor = LogAnchor::new(stop.entry.index, stop.entry.hash);
    let progress = learner
        .apply_page(
            &request,
            &CertifiedTailResponse {
                records: vec![CertifiedTailRecord {
                    entry: stop.entry.clone(),
                    proof: stop.proof.clone(),
                }],
                observed_tip: stop_anchor,
            },
        )
        .unwrap();
    assert_eq!(progress.durable, stop_anchor);
    assert_eq!(progress.applied, stop_anchor);

    let target_archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        store,
        CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 2, 1),
    );
    assert!(target_archive.load_checkpoint().await.unwrap().is_none());
    let peers = successor
        .members()
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            PeerConfig::new(
                node_id,
                format!("http://127.0.0.1:{}", 9201 + index),
                format!("successor-peer-token-{}", index + 1),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let successor_config = NodeConfig::new_with_configuration(
        "rhiza:sql:cluster-a",
        "node-4",
        target_data_dir,
        1,
        successor.clone(),
        source.configuration_state().unwrap(),
        peers,
        "successor-client-token",
    )
    .unwrap()
    .with_log_initial_configuration(ConfigurationState::active(1, predecessor.digest()))
    .with_predecessor_stop_entry(stop.entry.clone());
    let wrong_predecessor = Membership::new(["node-1", "node-2", "node-9"]).unwrap();
    let error = learner
        .finalize(
            HaStartupConfig::new(
                successor_config.clone(),
                target_archive.clone(),
                DurabilityMode::Sync,
                60_000,
                HaStartupMode::Rejoin,
            )
            .with_predecessor(HaPredecessor::new(wrong_predecessor.clone(), stop.clone())),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("InvalidTransition")
            || error
                .to_string()
                .contains("Stop binding conflicts with the prestage target")
            || error
                .to_string()
                .contains("predecessor membership or Stop binding"),
        "{error}"
    );
    let learner = HaSuccessorPrestageConfig::resume(
        successor_config.data_dir(),
        1,
        predecessor.clone(),
        "tail-token",
    )
    .unwrap()
    .publish(successor_config.data_dir())
    .unwrap();
    assert_eq!(learner.durable_anchor().unwrap(), stop_anchor);
    assert_eq!(learner.applied_anchor().unwrap(), stop_anchor);
    drop(learner);
    let prestage = inspect_successor_prestage(
        successor_config.data_dir(),
        ConfigurationState::active(1, predecessor.digest()),
    )
    .unwrap();
    let finalized = finalize_successor_prestage_for_stop(prestage, &stop, &predecessor).unwrap();
    drop(finalized);
    let error = HaStartupConfig::new(
        successor_config.clone(),
        target_archive.clone(),
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Rejoin,
    )
    .with_predecessor(HaPredecessor::new(wrong_predecessor, stop.clone()))
    .resume_finalized_successor_prestage()
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("InvalidTransition")
            || error
                .to_string()
                .contains("Stop binding conflicts with the prestage target")
            || error
                .to_string()
                .contains("predecessor membership or Stop binding"),
        "{error}"
    );
    assert!(HaSuccessorPrestageConfig::resume(
        successor_config.data_dir(),
        1,
        predecessor.clone(),
        "tail-token",
    )
    .is_err());
    let prepared = HaStartupConfig::new(
        successor_config.clone(),
        target_archive.clone(),
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Rejoin,
    )
    .with_predecessor(HaPredecessor::new(predecessor.clone(), stop.clone()))
    .resume_finalized_successor_prestage()
    .await
    .unwrap();
    let initialized = target_archive.load_checkpoint().await.unwrap().unwrap();
    assert_eq!(initialized.manifest().tip().index(), 0);
    assert!(initialized.manifest().segments().is_empty());
    drop(prepared);
    let prepared = HaStartupConfig::new(
        successor_config,
        target_archive,
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Rejoin,
    )
    .with_predecessor(HaPredecessor::new(predecessor.clone(), stop.clone()))
    .resume_finalized_successor_prestage()
    .await
    .unwrap();
    let recorder_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node = prepared
        .start(HaServeConfig::new(
            recorder_listener,
            service_listener,
            HaRecorderTransport::Http,
            successor_recorder_clients(
                &root.path().join("successor-recorders"),
                &predecessor,
                &successor,
                &stop,
            ),
            Vec::new(),
        ))
        .await
        .unwrap();

    assert!(!node.is_ready());
    tokio::select! {
        biased;
        _ = node.ready() => panic!("AwaitingActivation must not become ready"),
        () = std::future::ready(()) => {}
    }
    node.shutdown().await.unwrap();
}
