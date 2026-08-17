use std::{
    path::Path,
    sync::{mpsc, Arc, Condvar, Mutex},
    time::Duration,
};

use rhiza_core::{
    Command, CommandKind, ConfigChange, ConfigurationState, ExecutionProfile, LogHash,
};
use rhiza_node::{
    effective_cluster_id, install_successor_recorder, log_peer_router, AdminConfig, ConfigError,
    FetchLogRequest, InMemoryLogPeer, NodeConfig, NodeError, NodeRuntime, PeerConfig,
    RuntimeConfigurationStatus, LOG_FETCH_PATH, NODE_ID_HEADER, PROTOCOL_VERSION, VERSION_HEADER,
};
use rhiza_quepaxa::{
    DecisionProof, Membership, RecordRequest, RecordSummary, RecorderFileStore, RecorderRpc,
    ThreeNodeConsensus,
};
use tempfile::TempDir;

fn peers(count: usize) -> Vec<PeerConfig> {
    (1..=count)
        .map(|id| {
            PeerConfig::new(
                format!("n{id}"),
                format!("http://127.0.0.1:90{id:02}"),
                format!("peer-{id}"),
            )
            .unwrap()
        })
        .collect()
}

#[test]
fn peer_config_rejects_base_urls_that_cannot_be_safely_extended() {
    for url in [
        "http://127.0.0.1:9101/prefix",
        "http://127.0.0.1:9101?tenant=a",
        "http://127.0.0.1:9101#fragment",
        "http://user@127.0.0.1:9101",
        "http://user:password@127.0.0.1:9101",
    ] {
        assert!(matches!(
            PeerConfig::new("n1", url, "peer-1"),
            Err(rhiza_node::ConfigError::InvalidPeerBaseUrl(actual)) if actual == url
        ));
        assert!(matches!(
            PeerConfig::new_with_log_url("n1", "http://127.0.0.1:9101", url, "peer-1"),
            Err(rhiza_node::ConfigError::InvalidPeerBaseUrl(actual)) if actual == url
        ));
    }
}

#[test]
fn http_configuration_rejects_blank_or_unrepresentable_header_values() {
    for invalid in [" ", "line\nbreak", "café"] {
        assert!(matches!(
            PeerConfig::new(invalid, "http://127.0.0.1:9101", "peer-1"),
            Err(ConfigError::EmptyPeerNodeId)
        ));
        assert!(matches!(
            PeerConfig::new("n1", "http://127.0.0.1:9101", invalid),
            Err(ConfigError::EmptyPeerToken)
        ));
        assert!(matches!(
            NodeConfig::new(
                "rhiza:sql:cluster-a",
                "n1",
                std::path::PathBuf::from("node"),
                1,
                1,
                peers(3),
                invalid,
            ),
            Err(ConfigError::EmptyClientToken)
        ));
        assert!(matches!(
            AdminConfig::new(invalid),
            Err(ConfigError::EmptyAdminToken)
        ));
    }

    for invalid_token in [" secret ", "a b", "secret\tvalue"] {
        assert!(matches!(
            PeerConfig::new("n1", "http://127.0.0.1:9101", invalid_token),
            Err(ConfigError::EmptyPeerToken)
        ));
        assert!(matches!(
            NodeConfig::new(
                "rhiza:sql:cluster-a",
                "n1",
                std::path::PathBuf::from("node"),
                1,
                1,
                peers(3),
                invalid_token,
            ),
            Err(ConfigError::EmptyClientToken)
        ));
        assert!(matches!(
            AdminConfig::new(invalid_token),
            Err(ConfigError::EmptyAdminToken)
        ));
    }
}

#[test]
fn node_config_rejects_authentication_credential_collisions() {
    let duplicate_peer_token = vec![
        PeerConfig::new("n1", "http://127.0.0.1:9101", "shared").unwrap(),
        PeerConfig::new("n2", "http://127.0.0.1:9102", "shared").unwrap(),
        PeerConfig::new("n3", "http://127.0.0.1:9103", "peer-3").unwrap(),
    ];
    assert!(matches!(
        NodeConfig::new(
            "rhiza:sql:cluster-a",
            "n1",
            std::path::PathBuf::from("node"),
            1,
            1,
            duplicate_peer_token,
            "client-token",
        ),
        Err(ConfigError::DuplicatePeerToken)
    ));

    assert!(matches!(
        NodeConfig::new(
            "rhiza:sql:cluster-a",
            "n1",
            std::path::PathBuf::from("node"),
            1,
            1,
            peers(3),
            "peer-2",
        ),
        Err(ConfigError::ClientTokenConflictsWithPeer)
    ));
}

#[test]
fn node_config_accepts_four_unique_peers_and_uses_canonical_membership() {
    let root = TempDir::new().unwrap();
    let config = NodeConfig::new(
        "rhiza:sql:cluster-a",
        "n1",
        root.path().to_path_buf(),
        1,
        7,
        peers(4),
        "client-token",
    )
    .unwrap();
    let expected = Membership::from_voters((1..=4).map(|id| format!("n{id}"))).unwrap();

    assert_eq!(config.peers().len(), 4);
    assert_eq!(config.membership(), &expected);
    assert_eq!(
        config.configuration_state(),
        &ConfigurationState::active(7, expected.digest())
    );
}

#[test]
fn node_config_defaults_existing_constructors_to_sqlite_profile() {
    let root = TempDir::new().unwrap();
    let networked = NodeConfig::new(
        "rhiza:sql:cluster-a",
        "n1",
        root.path().join("networked"),
        1,
        1,
        peers(3),
        "client-token",
    )
    .unwrap();
    let embedded = NodeConfig::new_embedded(
        "rhiza:sql:cluster-a",
        "n1",
        root.path().join("embedded"),
        1,
        1,
        ["n1", "n2", "n3"],
    )
    .unwrap();

    assert_eq!(networked.execution_profile(), ExecutionProfile::Sqlite);
    assert_eq!(embedded.execution_profile(), ExecutionProfile::Sqlite);
}

#[test]
fn node_config_explicit_profile_builder_selects_graph() {
    let root = TempDir::new().unwrap();
    let config = NodeConfig::new(
        "cluster-a",
        "n1",
        root.path().join("node"),
        1,
        1,
        peers(3),
        "client-token",
    )
    .unwrap()
    .with_execution_profile(ExecutionProfile::Graph)
    .unwrap();

    assert_eq!(config.execution_profile(), ExecutionProfile::Graph);
}

#[test]
fn effective_cluster_id_accepts_logical_and_same_profile_canonical_inputs() {
    assert_eq!(
        effective_cluster_id(ExecutionProfile::Graph, "cluster-a").unwrap(),
        "rhiza:graph:cluster-a"
    );
    assert_eq!(
        effective_cluster_id(ExecutionProfile::Graph, "rhiza:graph:cluster-a").unwrap(),
        "rhiza:graph:cluster-a"
    );
}

#[test]
fn effective_cluster_id_rejects_a_different_canonical_profile() {
    assert_eq!(
        effective_cluster_id(ExecutionProfile::Graph, "rhiza:sql:cluster-a"),
        Err(ConfigError::ClusterIdProfileMismatch {
            expected: ExecutionProfile::Graph,
            actual: ExecutionProfile::Sqlite,
        })
    );
}

#[test]
fn profile_builder_rejects_rewriting_a_canonical_cluster_identity() {
    let root = TempDir::new().unwrap();
    let error = NodeConfig::new(
        "rhiza:sql:cluster-a",
        "n1",
        root.path().join("node"),
        1,
        1,
        peers(3),
        "client-token",
    )
    .unwrap()
    .with_execution_profile(ExecutionProfile::Graph)
    .unwrap_err();

    assert_eq!(
        error,
        ConfigError::ClusterIdProfileMismatch {
            expected: ExecutionProfile::Graph,
            actual: ExecutionProfile::Sqlite,
        }
    );
}

#[test]
#[cfg(not(feature = "graph"))]
fn graph_profile_requires_compiled_feature_before_creating_node_storage() {
    let root = TempDir::new().unwrap();
    let data_dir = root.path().join("graph-node");
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let config = NodeConfig::new(
        "cluster-a",
        "n1",
        data_dir.clone(),
        1,
        1,
        peers(3),
        "client-token",
    )
    .unwrap()
    .with_execution_profile(ExecutionProfile::Graph)
    .unwrap();

    let error = NodeRuntime::open(
        config,
        Arc::new(membership_consensus(
            &root.path().join("recorders"),
            membership,
        )),
        &[],
    )
    .unwrap_err();

    assert_eq!(
        error,
        NodeError::Unavailable("graph execution profile is not compiled in".to_owned())
    );
    assert!(!data_dir.exists());
}

#[test]
fn embedded_node_config_accepts_canonical_members_without_transport_config() {
    let root = TempDir::new().unwrap();
    let config = NodeConfig::new_embedded(
        "rhiza:sql:cluster-a",
        "n1",
        root.path().to_path_buf(),
        1,
        7,
        ["n3", "n1", "n2"],
    )
    .unwrap();
    let expected = Membership::new(["n1", "n2", "n3"]).unwrap();

    assert_eq!(config.membership(), &expected);
    assert_eq!(
        config.configuration_state(),
        &ConfigurationState::active(7, expected.digest())
    );
    assert!(config.peers().is_empty());
    assert!(config.client_token().is_empty());
}

#[test]
fn embedded_node_config_rejects_invalid_or_local_missing_membership() {
    let root = TempDir::new().unwrap();
    let invalid_count = NodeConfig::new_embedded(
        "rhiza:sql:cluster-a",
        "n1",
        root.path().join("count"),
        1,
        1,
        ["n1", "n2"],
    )
    .unwrap_err();
    let local_missing = NodeConfig::new_embedded(
        "rhiza:sql:cluster-a",
        "n1",
        root.path().join("missing"),
        1,
        1,
        ["n2", "n3", "n4"],
    )
    .unwrap_err();

    assert_eq!(invalid_count, ConfigError::InvalidPeerCount(2));
    assert_eq!(local_missing, ConfigError::LocalNodeMissing);
}

#[test]
fn node_config_rejects_membership_outside_three_to_seven() {
    let root = TempDir::new().unwrap();
    let error = NodeConfig::new(
        "rhiza:sql:cluster-a",
        "n1",
        root.path().to_path_buf(),
        1,
        1,
        peers(2),
        "client-token",
    )
    .unwrap_err();

    assert_eq!(error, ConfigError::InvalidPeerCount(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn fourth_peer_is_authorized_for_log_fetch() {
    let router = log_peer_router(InMemoryLogPeer::new(Vec::new()), peers(4));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

    let response = reqwest::Client::new()
        .post(format!("http://{address}{LOG_FETCH_PATH}"))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .header(NODE_ID_HEADER, "n4")
        .header("x-rhiza-recovery-generation", "1")
        .bearer_auth("peer-4")
        .json(&FetchLogRequest {
            from_index: 1,
            max_entries: 1,
        })
        .send()
        .await
        .unwrap();

    server.abort();
    assert!(response.status().is_success());
}

#[test]
fn stopped_runtime_rejects_writes_with_typed_transition_error() {
    let root = TempDir::new().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let config = NodeConfig::new(
        "rhiza:sql:cluster-a",
        "n1",
        root.path().join("node"),
        1,
        1,
        peers(3),
        "client-token",
    )
    .unwrap();
    let consensus = Arc::new(membership_consensus(root.path(), membership.clone()));
    let runtime = NodeRuntime::open(config, consensus, &[]).unwrap();
    runtime
        .stop_current_configuration_for_successor(&membership)
        .unwrap();
    let stopped = runtime.configuration_state().unwrap();

    assert_eq!(runtime.configuration_state().unwrap(), stopped);
    assert!(matches!(
        runtime.write("request-1", "key", "value"),
        Err(NodeError::ConfigurationTransition { state }) if state.as_ref() == &stopped
    ));
}

#[test]
fn stop_advances_past_an_already_decided_normal_command() {
    let root = TempDir::new().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let config = NodeConfig::new(
        "rhiza:sql:cluster-a",
        "n1",
        root.path().join("node"),
        1,
        1,
        peers(3),
        "client-token",
    )
    .unwrap();
    let consensus = Arc::new(membership_consensus(root.path(), membership.clone()));
    consensus
        .propose_at(
            rhiza_quepaxa::RecorderRpcContext::default_timeout(),
            1,
            LogHash::ZERO,
            Command::new(CommandKind::ReadBarrier, Vec::new()),
        )
        .unwrap();
    let runtime = NodeRuntime::open(config, consensus, &[]).unwrap();

    let stop = runtime
        .stop_current_configuration_for_successor(&membership)
        .unwrap();

    assert_eq!(stop.entry.index, 2);
    assert_eq!(runtime.applied_index().unwrap(), 2);
    assert_eq!(
        runtime
            .configuration_state()
            .unwrap()
            .stop()
            .unwrap()
            .index(),
        2
    );
}

#[test]
fn write_rechecks_stop_after_reconciling_a_foreign_slot() {
    let root = TempDir::new().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let config = NodeConfig::new(
        "rhiza:sql:cluster-a",
        "n1",
        root.path().join("node"),
        1,
        1,
        peers(3),
        "client-token",
    )
    .unwrap();
    let consensus = Arc::new(membership_consensus(root.path(), membership.clone()));
    let stop_command = ConfigChange::bound_stop(
        "rhiza:sql:cluster-a",
        1,
        membership.digest(),
        2,
        membership.members().to_vec(),
    )
    .unwrap()
    .to_stored_command();
    let stop = consensus
        .propose_stored_at(
            rhiza_quepaxa::RecorderRpcContext::default_timeout(),
            1,
            LogHash::ZERO,
            stop_command,
        )
        .unwrap();
    let runtime = NodeRuntime::open(config, consensus, &[]).unwrap();

    let error = runtime
        .write("request-after-stop", "key", "value")
        .unwrap_err();

    assert!(matches!(
        error,
        NodeError::ConfigurationTransition { ref state }
            if state.stop().is_some_and(|anchor| anchor.index() == stop.index)
    ));
    assert!(!runtime.is_fatal());
    assert_eq!(runtime.applied_index().unwrap(), stop.index);
}

#[test]
fn three_to_three_replacement_keeps_stopped_predecessor_alive_until_successor_is_ready() {
    let root = TempDir::new().unwrap();
    let old_membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let old_config = NodeConfig::new(
        "rhiza:sql:cluster-a",
        "n1",
        root.path().join("node"),
        1,
        1,
        peers(3),
        "client-token",
    )
    .unwrap();
    let old_consensus = Arc::new(membership_consensus(root.path(), old_membership.clone()));
    let old_runtime = NodeRuntime::open(old_config, old_consensus.clone(), &[]).unwrap();
    let successor_data_dir = root.path().join("successor-node");
    let successor_predecessor = NodeRuntime::open(
        NodeConfig::new(
            "rhiza:sql:cluster-a",
            "n1",
            successor_data_dir.clone(),
            1,
            1,
            peers(3),
            "client-token",
        )
        .unwrap(),
        old_consensus,
        &[],
    )
    .unwrap();
    old_runtime.write("request-1", "before", "stop").unwrap();
    let new_membership = Membership::new(["n1", "n2", "n4"]).unwrap();
    let stop = old_runtime
        .stop_current_configuration_for_successor(&new_membership)
        .unwrap();
    let stopped = old_runtime.configuration_state().unwrap();
    assert_eq!(stopped.stop().unwrap().index(), stop.entry.index);
    assert!(old_runtime
        .consensus()
        .propose_at(
            rhiza_quepaxa::RecorderRpcContext::default_timeout(),
            stop.entry.index + 1,
            stop.entry.hash,
            Command::new(CommandKind::ReadBarrier, Vec::new())
        )
        .is_err());
    while successor_predecessor.materialize_next_decision().unwrap() {}
    assert_eq!(
        successor_predecessor.configuration_state().unwrap(),
        stopped
    );
    drop(successor_predecessor);

    let recorder_ids = ["n1", "n2", "n4"];
    let mut successor_recorders: Vec<(String, Box<dyn RecorderRpc>)> = Vec::new();
    for recorder_id in recorder_ids {
        let recorder = RecorderFileStore::new_with_membership(
            root.path().join(format!("successor-{recorder_id}")),
            recorder_id,
            "rhiza:sql:cluster-a",
            1,
            1,
            old_membership.clone(),
        )
        .unwrap();
        install_successor_recorder(&recorder, 2, new_membership.clone(), &stop).unwrap();
        successor_recorders.push((recorder_id.to_string(), Box::new(recorder)));
    }
    assert!(matches!(
        old_runtime.write("request-2", "after", "install"),
        Err(NodeError::ConfigurationTransition { state }) if state.as_ref() == &stopped
    ));
    let successor_consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "n1",
            1,
            2,
            successor_recorders,
        )
        .unwrap(),
    );
    let successor_peers = vec![
        PeerConfig::new("n1", "http://127.0.0.1:9101", "peer-1").unwrap(),
        PeerConfig::new("n2", "http://127.0.0.1:9102", "peer-2").unwrap(),
        PeerConfig::new("n4", "http://127.0.0.1:9104", "peer-4").unwrap(),
    ];
    let successor_config = NodeConfig::new_with_configuration(
        "rhiza:sql:cluster-a",
        "n1",
        successor_data_dir.clone(),
        1,
        new_membership.clone(),
        stopped.clone(),
        successor_peers.clone(),
        "client-token",
    )
    .unwrap()
    .with_log_initial_configuration(ConfigurationState::active(1, old_membership.digest()));
    let successor = NodeRuntime::open(successor_config, successor_consensus, &[]).unwrap();
    assert_eq!(
        successor.status().unwrap().configuration_status,
        RuntimeConfigurationStatus::AwaitingActivation
    );
    drop(old_runtime);
    let activation = successor.activate_successor().unwrap();
    assert_eq!(activation.index, stop.entry.index + 1);
    assert_eq!(activation.config_id, 2);
    assert_eq!(
        successor.status().unwrap().configuration_status,
        RuntimeConfigurationStatus::Active
    );
    successor.write("request-3", "after", "activation").unwrap();
    drop(successor);

    let restarted_recorders: Vec<(String, Box<dyn RecorderRpc>)> = recorder_ids
        .into_iter()
        .map(|recorder_id| {
            let recorder = RecorderFileStore::new_with_membership(
                root.path().join(format!("successor-{recorder_id}")),
                recorder_id,
                "rhiza:sql:cluster-a",
                1,
                2,
                new_membership.clone(),
            )
            .unwrap();
            (
                recorder_id.to_string(),
                Box::new(recorder) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    let restarted_consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "n1",
            1,
            2,
            restarted_recorders,
        )
        .unwrap(),
    );
    let restarted_config = NodeConfig::new(
        "rhiza:sql:cluster-a",
        "n1",
        successor_data_dir,
        1,
        2,
        successor_peers,
        "client-token",
    )
    .unwrap()
    .with_log_initial_configuration(ConfigurationState::active(1, old_membership.digest()));
    let restarted = NodeRuntime::open(restarted_config, restarted_consensus, &[]).unwrap();
    assert_eq!(
        restarted.status().unwrap().configuration_status,
        RuntimeConfigurationStatus::Active
    );
    restarted.write("request-4", "restart", "active").unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn two_runtimes_materialize_same_voter_stop_and_activation_in_background() {
    let root = TempDir::new().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let stores = ["n1", "n2", "n3"]
        .into_iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.path().join(format!("shared-{id}")),
                id,
                "rhiza:sql:cluster-a",
                1,
                1,
                membership.clone(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let open_old = |name: &str| {
        Arc::new(
            NodeRuntime::open(
                NodeConfig::new(
                    "rhiza:sql:cluster-a",
                    "n1",
                    root.path().join(name),
                    1,
                    1,
                    peers(3),
                    "client-token",
                )
                .unwrap(),
                Arc::new(consensus_from_stores(1, &stores)),
                &[],
            )
            .unwrap(),
        )
    };
    let proposer = open_old("proposer");
    let follower = open_old("follower");
    let (stop_shutdown, stop_signal) = tokio::sync::oneshot::channel();
    let stop_worker = tokio::spawn(follower.clone().run_background_materializer(
        Duration::from_millis(10),
        async move {
            let _ = stop_signal.await;
        },
    ));

    let stop = proposer
        .stop_current_configuration_for_successor(&membership)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while follower.configuration_state().unwrap().is_active() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    stop_shutdown.send(()).unwrap();
    stop_worker.await.unwrap().unwrap();
    let stopped = follower.configuration_state().unwrap();
    drop(proposer);
    drop(follower);

    for store in &stores {
        install_successor_recorder(store, 2, membership.clone(), &stop).unwrap();
    }
    let open_successor = |name: &str| {
        Arc::new(
            NodeRuntime::open(
                NodeConfig::new_with_configuration(
                    "rhiza:sql:cluster-a",
                    "n1",
                    root.path().join(name),
                    1,
                    membership.clone(),
                    stopped.clone(),
                    peers(3),
                    "client-token",
                )
                .unwrap()
                .with_log_initial_configuration(ConfigurationState::active(1, membership.digest())),
                Arc::new(consensus_from_stores(2, &stores)),
                &[],
            )
            .unwrap(),
        )
    };
    let proposer = open_successor("proposer");
    let follower = open_successor("follower");
    let (activation_shutdown, activation_signal) = tokio::sync::oneshot::channel();
    let activation_worker = tokio::spawn(follower.clone().run_background_materializer(
        Duration::from_millis(10),
        async move {
            let _ = activation_signal.await;
        },
    ));

    proposer.activate_successor().unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !follower.configuration_state().unwrap().is_active() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    activation_shutdown.send(()).unwrap();
    activation_worker.await.unwrap().unwrap();
    assert_eq!(follower.configuration_state().unwrap().config_id(), 2);
}

#[test]
fn admitted_slow_minority_keeps_write_pending_until_mutation_drains() {
    let root = TempDir::new().unwrap();
    let membership = Membership::new(["n1", "n2", "n3"]).unwrap();
    let slow_gate = Arc::new(SlowGate::default());
    let mut recorders: Vec<(String, Box<dyn RecorderRpc>)> = Vec::new();
    for id in ["n1", "n2", "n3"] {
        let recorder = RecorderFileStore::new_with_membership(
            root.path().join(format!("slow-{id}")),
            id,
            "rhiza:sql:cluster-a",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let recorder: Box<dyn RecorderRpc> = if id == "n3" {
            Box::new(SlowRecorder {
                inner: recorder,
                gate: slow_gate.clone(),
            })
        } else {
            Box::new(recorder)
        };
        recorders.push((id.to_string(), recorder));
    }
    let runtime = NodeRuntime::open(
        NodeConfig::new(
            "rhiza:sql:cluster-a",
            "n1",
            root.path().join("slow-node"),
            1,
            1,
            peers(3),
            "client-token",
        )
        .unwrap(),
        Arc::new(
            ThreeNodeConsensus::from_recorders_with_ids(
                "rhiza:sql:cluster-a",
                "n1",
                1,
                1,
                recorders,
            )
            .unwrap(),
        ),
        &[],
    )
    .unwrap();

    let (completed_tx, completed_rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        let result = runtime.write("request-1", "alpha", "one");
        completed_tx.send(result).unwrap();
    });

    slow_gate.wait_until_entered();
    // A quorum freezes the proposal result, but an already admitted mutating
    // hedge must reach a terminal state before the write can return.  Returning
    // while n3 is still running would make the successful write outcome race a
    // later, potentially ambiguous mutation.
    assert_eq!(
        completed_rx.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout),
        "write returned while the admitted minority mutation was still running"
    );
    slow_gate.release();
    completed_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("write did not complete after the admitted minority mutation drained")
        .unwrap();
    writer.join().unwrap();
}

#[derive(Default)]
struct SlowGate {
    entered: Mutex<bool>,
    entered_cv: Condvar,
    released: Mutex<bool>,
    released_cv: Condvar,
}

impl SlowGate {
    fn block(&self) {
        *self.entered.lock().unwrap() = true;
        self.entered_cv.notify_all();
        let released = self.released.lock().unwrap();
        drop(
            self.released_cv
                .wait_while(released, |released| !*released)
                .unwrap(),
        );
    }

    fn wait_until_entered(&self) {
        let entered = self.entered.lock().unwrap();
        drop(
            self.entered_cv
                .wait_while(entered, |entered| !*entered)
                .unwrap(),
        );
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.released_cv.notify_all();
    }
}

#[derive(Clone)]
struct SlowRecorder {
    inner: RecorderFileStore,
    gate: Arc<SlowGate>,
}

impl RecorderRpc for SlowRecorder {
    fn recorder_id(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        self.inner.recorder_id()
    }

    fn store_command_for(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
        command: rhiza_core::StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        self.inner.store_command_for(
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
            command,
        )
    }

    fn fetch_command_for(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<rhiza_core::StoredCommand>> {
        self.inner
            .fetch_command_for(cluster_id, epoch, config_id, config_digest, command_hash)
    }

    fn record(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        request: RecordRequest,
    ) -> rhiza_quepaxa::Result<RecordSummary> {
        self.gate.block();
        self.inner.record(request)
    }

    fn install_decision_proof(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        self.inner.install_decision_proof(proof, membership)
    }

    fn inspect_decision_proof(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        self.inner.inspect_decision_proof(slot)
    }

    fn inspect_record_summary(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        self.inner.inspect_record_summary(slot)
    }
}

fn consensus_from_stores(config_id: u64, stores: &[RecorderFileStore]) -> ThreeNodeConsensus {
    let recorders = stores
        .iter()
        .map(|store| {
            (
                store.recorder_id().unwrap(),
                Box::new(store.clone()) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    ThreeNodeConsensus::from_recorders_with_ids(
        "rhiza:sql:cluster-a",
        "n1",
        1,
        config_id,
        recorders,
    )
    .unwrap()
}

fn membership_consensus(root: &Path, membership: Membership) -> ThreeNodeConsensus {
    let recorders: Vec<(String, Box<dyn RecorderRpc>)> = ["n1", "n2", "n3"]
        .into_iter()
        .map(|id| {
            let recorder = RecorderFileStore::new_with_membership(
                root.join(format!("old-{id}")),
                id,
                "rhiza:sql:cluster-a",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (id.to_string(), Box::new(recorder) as Box<dyn RecorderRpc>)
        })
        .collect();
    ThreeNodeConsensus::from_recorders_with_ids("rhiza:sql:cluster-a", "n1", 1, 1, recorders)
        .unwrap()
}
