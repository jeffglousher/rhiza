use std::{path::Path, sync::Arc, time::Duration};

use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
use rhiza_core::{
    Command, CommandKind, ConfigChange, ConfigurationState, ExecutionProfile, LogAnchor, LogHash,
};
use rhiza_log::FileLogStore;
use rhiza_node::{
    durability::{prestage_successor_checkpoint, publish_successor_prestage},
    CertifiedTailRecord, CertifiedTailRequest, CertifiedTailResponse, LearnerStore,
    LearnerStoreError, NodeConfig, PeerConfig, StopInformation, TailReaderConfig,
};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{CertifiedDecisionInspection, Membership, ThreeNodeConsensus};
use rhiza_sql::SqliteStateMachine;

const CLUSTER_ID: &str = "rhiza:sql:cluster-a";
const EPOCH: u64 = 9;
const PREDECESSOR_CONFIG_ID: u64 = 4;
const RECOVERY_GENERATION: u64 = 7;

async fn initialized_archive(root: &Path) -> ObjectArchiveStore {
    let archive = ObjectArchiveStore::new_checkpoint_for_single_process(
        ObjStore::new(ObjStoreConfig::Local {
            root: root.to_path_buf(),
        })
        .unwrap(),
        CheckpointIdentity::new(
            CLUSTER_ID,
            EPOCH,
            PREDECESSOR_CONFIG_ID,
            RECOVERY_GENERATION,
        ),
    );
    archive.initialize_checkpoint().await.unwrap();
    archive
}

fn peers(membership: &Membership) -> Vec<PeerConfig> {
    membership
        .members()
        .iter()
        .enumerate()
        .map(|(index, node)| {
            PeerConfig::new(
                node,
                format!("http://{node}:8081"),
                format!("peer-token-{index}"),
            )
            .unwrap()
        })
        .collect()
}

fn certified(consensus: &ThreeNodeConsensus, entry: &rhiza_core::LogEntry) -> CertifiedTailRecord {
    let CertifiedDecisionInspection::Committed(certified) = consensus
        .inspect_certified_decision_at(entry.index, entry.prev_hash)
        .unwrap()
    else {
        panic!("test decision was not certified");
    };
    assert_eq!(certified.entry, *entry);
    CertifiedTailRecord {
        entry: entry.clone(),
        proof: certified.proof,
    }
}

#[tokio::test]
async fn detached_learner_applies_only_certified_pages_and_adopts_the_exact_bound_stop() {
    let root = tempfile::tempdir().unwrap();
    let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let successor = Membership::new(["node-4", "node-5", "node-6"]).unwrap();
    let archive = initialized_archive(&root.path().join("archive")).await;
    let prestage = prestage_successor_checkpoint(
        archive,
        root.path().join("prestage"),
        ConfigurationState::active(PREDECESSOR_CONFIG_ID, predecessor.digest()),
        "node-4",
        ExecutionProfile::Sqlite,
        PREDECESSOR_CONFIG_ID + 1,
        successor.digest(),
    )
    .await
    .unwrap();
    let data_dir = root.path().join("node-4");
    let published = publish_successor_prestage(prestage, &data_dir).unwrap();
    drop(published);

    let sqlite_dir = data_dir.join("sqlite");
    if sqlite_dir.exists() {
        std::fs::remove_dir_all(&sqlite_dir).unwrap();
    }
    drop(
        SqliteStateMachine::open_with_configuration(
            data_dir.join("sqlite/db.sqlite"),
            CLUSTER_ID,
            "node-4",
            EPOCH,
            ConfigurationState::active(PREDECESSOR_CONFIG_ID, predecessor.digest()),
        )
        .unwrap(),
    );

    let consensus = Arc::new(
        ThreeNodeConsensus::new(
            CLUSTER_ID,
            "node-1",
            EPOCH,
            PREDECESSOR_CONFIG_ID,
            [
                root.path().join("node-1"),
                root.path().join("node-2"),
                root.path().join("node-3"),
            ],
        )
        .unwrap(),
    );
    let noop = consensus
        .propose_at(
            1,
            LogHash::ZERO,
            Command::new(CommandKind::ReadBarrier, Vec::new()),
        )
        .unwrap();
    let stop_command = ConfigChange::stop(
        CLUSTER_ID,
        PREDECESSOR_CONFIG_ID,
        predecessor.digest(),
        PREDECESSOR_CONFIG_ID + 1,
        successor.members().to_vec(),
    )
    .unwrap()
    .to_stored_command();
    let stop_command_hash = stop_command.hash();
    let successor_descriptor = ConfigChange::recognize(&stop_command)
        .unwrap()
        .successor()
        .clone();
    let stop_entry = consensus
        .propose_stored_at(2, noop.hash, stop_command)
        .unwrap();
    assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
    let noop_record = certified(&consensus, &noop);
    let stop_record = certified(&consensus, &stop_entry);
    let stop = StopInformation {
        entry: stop_entry.clone(),
        proof: stop_record.proof.clone(),
    };
    let stop_anchor = LogAnchor::new(stop_entry.index, stop_entry.hash);
    let reader = TailReaderConfig::new(
        CLUSTER_ID,
        EPOCH,
        PREDECESSOR_CONFIG_ID,
        predecessor.clone(),
        RECOVERY_GENERATION,
        "tail-token",
    )
    .unwrap();

    let store = LearnerStore::open(&data_dir, reader.clone()).unwrap();
    let request = CertifiedTailRequest {
        from: LogAnchor::new(0, LogHash::ZERO),
        max_entries: 2,
    };
    let mut divergent = CertifiedTailResponse {
        records: vec![noop_record.clone(), stop_record.clone()],
        observed_tip: stop_anchor,
    };
    divergent.records[0].entry.payload.push(1);
    assert!(matches!(
        store.apply_page(&request, &divergent),
        Err(LearnerStoreError::InvalidPage(_))
    ));
    assert_eq!(
        store.durable_anchor().unwrap(),
        LogAnchor::new(0, LogHash::ZERO)
    );

    let first_page = CertifiedTailResponse {
        records: vec![noop_record],
        observed_tip: stop_anchor,
    };
    let progress = store.apply_page(&request, &first_page).unwrap();
    assert_eq!(progress.durable, LogAnchor::new(noop.index, noop.hash));
    assert_eq!(progress.applied, progress.durable);
    drop(store);

    // Simulate a crash after the accepted page's qlog sync and before materialization.
    let detached_log = FileLogStore::open_with_configuration(
        data_dir.join("consensus/log"),
        CLUSTER_ID,
        EPOCH,
        ConfigurationState::active(PREDECESSOR_CONFIG_ID, predecessor.digest()),
    )
    .unwrap();
    detached_log
        .append_batch_buffered(std::slice::from_ref(&stop_entry))
        .unwrap();
    detached_log.sync().unwrap();
    let stopped_configuration = ConfigurationState::stopped(
        PREDECESSOR_CONFIG_ID,
        predecessor.digest(),
        stop_anchor,
        successor_descriptor,
        stop_command_hash,
    );
    assert_eq!(
        detached_log.configuration_state().unwrap(),
        stopped_configuration
    );
    drop(detached_log);

    let successor_config = NodeConfig::new_with_configuration(
        CLUSTER_ID,
        "node-4",
        data_dir.clone(),
        EPOCH,
        successor.clone(),
        stopped_configuration,
        peers(&successor),
        "client-token",
    )
    .unwrap()
    .with_log_initial_configuration(ConfigurationState::active(
        PREDECESSOR_CONFIG_ID,
        predecessor.digest(),
    ))
    .with_predecessor_stop_entry(stop_entry.clone())
    .with_recovery_generation(RECOVERY_GENERATION)
    .unwrap();

    let resumed = LearnerStore::open(&data_dir, reader).unwrap();
    assert_eq!(resumed.durable_anchor().unwrap(), stop_anchor);
    assert_eq!(resumed.applied_anchor().unwrap(), stop_anchor);

    let stale_request = CertifiedTailRequest {
        from: LogAnchor::new(noop.index, noop.hash),
        max_entries: 1,
    };
    assert_eq!(
        resumed.apply_page(
            &stale_request,
            &CertifiedTailResponse {
                records: vec![stop_record],
                observed_tip: stop_anchor,
            },
        ),
        Err(LearnerStoreError::ReanchorRequired(stop_anchor))
    );
    let retry = resumed.tail_request(1).unwrap();
    assert_eq!(retry.from, stop_anchor);
    assert_eq!(retry.max_entries, 1);

    let preparation = resumed.finalize(&successor_config, &stop).unwrap();
    assert_eq!(
        preparation.tip(),
        rhiza_archive::CheckpointTip::new(stop_anchor.index(), stop_anchor.hash())
    );
    assert!(preparation.requires_recorder_install());
}
