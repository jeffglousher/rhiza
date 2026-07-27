use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

use rhiza_core::{Command, CommandKind, EntryType, LogHash, StoredCommand};
use rhiza_node::{
    serve_recorder_tcp, serve_recorder_tcp_tls, PeerConfig, RecorderTlsClientConfig,
    RecorderTlsServerConfig, TcpPostcardRecorderClient,
};
use rhiza_quepaxa::{
    AcceptedValue, CertifiedDecisionInspection, DecisionProof, Error, Membership, Proposal,
    ProposalPriority, ReadFenceRequest, ReadFenceSlotState, RecordRequest, RecordSummary,
    RecorderFileStore, RecorderRpc, RejectReason, ThreeNodeConsensus,
};

fn peers() -> Vec<PeerConfig> {
    (1..=3)
        .map(|index| {
            PeerConfig::new(
                format!("node-{index}"),
                format!("http://node-{index}:8081"),
                format!("peer-token-{index}"),
            )
            .unwrap()
        })
        .collect()
}

fn tls_material(name: &str) -> (String, String) {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec![name.to_string()]).unwrap();
    (cert.pem(), signing_key.serialize_pem())
}

async fn tls_server(
    recorder: RecorderFileStore,
    cert_pem: &str,
    key_pem: &str,
) -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<Result<(), String>>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let config =
        RecorderTlsServerConfig::from_pem(cert_pem.as_bytes(), key_pem.as_bytes()).unwrap();
    let server = tokio::spawn(serve_recorder_tcp_tls(
        listener,
        recorder,
        peers(),
        7,
        config,
        std::future::pending(),
    ));
    (address, server)
}

fn recorder(root: &std::path::Path) -> RecorderFileStore {
    RecorderFileStore::new_with_id(root.join("recorder"), "node-1", "rhiza:sql:cluster-a", 1, 1)
        .unwrap()
}

#[derive(Clone, Default)]
struct CountingRecorder {
    records: Arc<Mutex<usize>>,
    proofs: Arc<Mutex<usize>>,
}

impl RecorderRpc for CountingRecorder {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        *self.records.lock().unwrap() += 1;
        Ok(RecordSummary {
            recorder_id: "node-1".into(),
            slot: request.slot,
            config_id: request.config_id,
            config_digest: request.config_digest,
            step: request.step,
            first_current: Some(request.proposal),
            aggregate_prior: None,
            decided: None,
        })
    }

    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        _membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        *self.proofs.lock().unwrap() += 1;
        Ok(())
    }
}

fn record_request(proposer_id: &str, slot: u64) -> RecordRequest {
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let command = StoredCommand::new(EntryType::Command, format!("command-{slot}").into_bytes());
    RecordRequest {
        cluster_id: "rhiza:sql:cluster-a".into(),
        epoch: 1,
        config_id: 1,
        config_digest: membership.digest(),
        slot,
        step: 4,
        proposal: Proposal::new(
            ProposalPriority::MAX,
            proposer_id,
            slot,
            AcceptedValue::from_command("rhiza:sql:cluster-a", slot, 1, 1, LogHash::ZERO, &command),
        ),
        command: Some(command),
    }
}

fn decision_proof(proposer_id: &str, slot: u64) -> DecisionProof {
    let request = record_request(proposer_id, slot);
    DecisionProof::FastPath {
        cluster_id: request.cluster_id,
        slot: request.slot,
        epoch: request.epoch,
        config_id: request.config_id,
        config_digest: request.config_digest,
        proposal: request.proposal,
        summaries: Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn recorder_tcp_accepts_member_relay_and_rejects_non_member_without_backend_call() {
    let recorder = CountingRecorder::default();
    let records = Arc::clone(&recorder.records);
    let proofs = Arc::clone(&recorder.proofs);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_recorder_tcp(
        listener,
        recorder,
        peers(),
        7,
        std::future::pending(),
    ));

    tokio::task::spawn_blocking(move || {
        let client =
            TcpPostcardRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7).unwrap();
        assert_eq!(client.record(record_request("node-1", 1)).unwrap().slot, 1);
        assert!(matches!(
            client.record(record_request("node-9", 2)),
            Err(Error::Rejected(RejectReason::InvalidRequest))
        ));
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        client
            .install_decision_proof(decision_proof("node-1", 3), &membership)
            .unwrap();
        assert!(matches!(
            client.install_decision_proof(decision_proof("node-9", 4), &membership),
            Err(Error::Rejected(RejectReason::InvalidRequest))
        ));
        assert_eq!(client.recorder_id().unwrap(), "node-1");
    })
    .await
    .unwrap();

    assert_eq!(*records.lock().unwrap(), 1);
    assert_eq!(*proofs.lock().unwrap(), 1);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn recorder_tls_round_trips_with_a_matching_ca_and_server_name() {
    let root = tempfile::tempdir().unwrap();
    let (cert_pem, key_pem) = tls_material("recorder.test");
    let (address, server) = tls_server(recorder(root.path()), &cert_pem, &key_pem).await;
    let tls = RecorderTlsClientConfig::from_ca_pem(cert_pem.as_bytes(), "recorder.test").unwrap();
    let client =
        TcpPostcardRecorderClient::new_tls(address, "node-1", "node-2", "peer-token-2", 7, tls)
            .unwrap();

    let identity = tokio::task::spawn_blocking(move || client.recorder_id())
        .await
        .unwrap();
    assert_eq!(identity.unwrap(), "node-1");
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn recorder_tls_rejects_an_untrusted_ca_and_wrong_server_name() {
    let root = tempfile::tempdir().unwrap();
    let (cert_pem, key_pem) = tls_material("recorder.test");
    let (other_ca, _) = tls_material("other.test");
    let (address, server) = tls_server(recorder(root.path()), &cert_pem, &key_pem).await;

    for tls in [
        RecorderTlsClientConfig::from_ca_pem(other_ca.as_bytes(), "recorder.test").unwrap(),
        RecorderTlsClientConfig::from_ca_pem(cert_pem.as_bytes(), "wrong.test").unwrap(),
    ] {
        let client =
            TcpPostcardRecorderClient::new_tls(address, "node-1", "node-2", "peer-token-2", 7, tls)
                .unwrap();
        assert!(tokio::task::spawn_blocking(move || client.recorder_id())
            .await
            .unwrap()
            .is_err());
    }
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn recorder_tls_and_plaintext_never_fall_back_to_each_other() {
    let root = tempfile::tempdir().unwrap();
    let (cert_pem, key_pem) = tls_material("recorder.test");
    let (tls_address, tls_server) =
        tls_server(recorder(&root.path().join("tls")), &cert_pem, &key_pem).await;
    let plain_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plain_address = plain_listener.local_addr().unwrap();
    let plain_server = tokio::spawn(serve_recorder_tcp(
        plain_listener,
        recorder(&root.path().join("plain")),
        peers(),
        7,
        std::future::pending(),
    ));
    let tls = RecorderTlsClientConfig::from_ca_pem(cert_pem.as_bytes(), "recorder.test").unwrap();
    let tls_to_plain = TcpPostcardRecorderClient::new_tls(
        plain_address,
        "node-1",
        "node-2",
        "peer-token-2",
        7,
        tls,
    )
    .unwrap();
    let plain_to_tls =
        TcpPostcardRecorderClient::new(tls_address, "node-1", "node-2", "peer-token-2", 7).unwrap();

    assert!(
        tokio::task::spawn_blocking(move || tls_to_plain.recorder_id())
            .await
            .unwrap()
            .is_err()
    );
    assert!(
        tokio::task::spawn_blocking(move || plain_to_tls.recorder_id())
            .await
            .unwrap()
            .is_err()
    );
    tls_server.abort();
    plain_server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn recorder_tls_rejects_bad_hello_after_a_valid_handshake() {
    let root = tempfile::tempdir().unwrap();
    let (cert_pem, key_pem) = tls_material("recorder.test");
    let (address, server) = tls_server(recorder(root.path()), &cert_pem, &key_pem).await;
    let tls = RecorderTlsClientConfig::from_ca_pem(cert_pem.as_bytes(), "recorder.test").unwrap();
    let client =
        TcpPostcardRecorderClient::new_tls(address, "node-1", "node-2", "wrong-token", 7, tls)
            .unwrap();

    assert!(tokio::task::spawn_blocking(move || client.recorder_id())
        .await
        .unwrap()
        .is_err());
    server.abort();
}

#[test]
fn recorder_tls_configuration_rejects_invalid_pem_empty_roots_and_mismatched_keys() {
    let (cert_pem, _) = tls_material("recorder.test");
    let (_, other_key_pem) = tls_material("other.test");

    assert!(RecorderTlsClientConfig::from_ca_pem(b"not pem", "recorder.test").is_err());
    assert!(RecorderTlsClientConfig::from_ca_pem(b"", "recorder.test").is_err());
    assert!(RecorderTlsClientConfig::from_ca_pem(cert_pem.as_bytes(), "bad name /").is_err());
    assert!(RecorderTlsServerConfig::from_pem(b"not pem", other_key_pem.as_bytes()).is_err());
    assert!(RecorderTlsServerConfig::from_pem(cert_pem.as_bytes(), b"not pem").is_err());
    assert!(
        RecorderTlsServerConfig::from_pem(cert_pem.as_bytes(), other_key_pem.as_bytes()).is_err()
    );
}

#[test]
fn recorder_tcp_client_creation_succeeds_before_peer_dns_is_published() {
    let client = TcpPostcardRecorderClient::new(
        "not-yet-published.invalid:8082",
        "node-1",
        "node-2",
        "peer-token-2",
        1,
    );

    assert!(client.is_ok());
}

#[derive(Clone)]
struct BlockingStore {
    inner: RecorderFileStore,
    started: mpsc::Sender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl RecorderRpc for BlockingStore {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        self.inner.recorder_id()
    }

    fn store_command_for(
        &self,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        let _ = self.started.send(());
        let (released, ready) = &*self.release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        self.inner.store_command_for(
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
            command,
        )
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn recorder_tcp_round_trips_identity_store_and_fetch() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let config_digest = membership.digest();
    let recorder = RecorderFileStore::new_with_membership(
        root.path().join("recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
        membership,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_recorder_tcp(
        listener,
        recorder,
        peers(),
        7,
        std::future::pending(),
    ));
    let client = Arc::new(
        TcpPostcardRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7).unwrap(),
    );
    let command = StoredCommand::new(EntryType::Command, b"command".to_vec());
    let hash = command.hash();

    tokio::task::spawn_blocking(move || {
        assert_eq!(client.recorder_id().unwrap(), "node-1");
        assert_eq!(
            client
                .observe_read_fence(ReadFenceRequest {
                    cluster_id: "rhiza:sql:cluster-a".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest,
                    slot: 1,
                })
                .unwrap()
                .slot_state,
            ReadFenceSlotState::Empty
        );
        client
            .store_command_for(
                "rhiza:sql:cluster-a".into(),
                1,
                1,
                config_digest,
                hash,
                command.clone(),
            )
            .unwrap();
        assert_eq!(
            client
                .fetch_command_for("rhiza:sql:cluster-a".into(), 1, 1, config_digest, hash,)
                .unwrap(),
            Some(command.clone())
        );
        client.record(record_request("node-1", 1)).unwrap();
        assert!(matches!(
            client
                .observe_read_fence(ReadFenceRequest {
                    cluster_id: "rhiza:sql:cluster-a".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest,
                    slot: 1,
                })
                .unwrap()
                .slot_state,
            ReadFenceSlotState::Occupied {
                summary: Some(summary)
            } if summary.slot == 1
        ));
    })
    .await
    .unwrap();
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn three_tcp_recorders_reconstruct_ordinary_proof_from_typed_summaries() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let mut servers = Vec::new();
    let mut addresses = Vec::new();
    for index in 1..=3 {
        let recorder = RecorderFileStore::new_with_membership(
            root.path().join(format!("recorder-{index}")),
            format!("node-{index}"),
            "rhiza:sql:cluster-a",
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(listener.local_addr().unwrap());
        servers.push(tokio::spawn(serve_recorder_tcp(
            listener,
            recorder,
            peers(),
            1,
            std::future::pending(),
        )));
    }
    let recorders = addresses
        .iter()
        .enumerate()
        .map(|(index, address)| {
            let recorder_id = format!("node-{}", index + 1);
            let client = TcpPostcardRecorderClient::new(
                *address,
                recorder_id.clone(),
                "node-1",
                "peer-token-1",
                1,
            )
            .unwrap();
            (recorder_id, Box::new(client) as Box<dyn RecorderRpc>)
        })
        .collect();
    let first_address = addresses[0];
    let install_membership = membership.clone();

    tokio::task::spawn_blocking(move || {
        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
            recorders,
        )
        .unwrap();
        let committed = consensus
            .propose_at(
                1,
                LogHash::ZERO,
                Command::new(CommandKind::Deterministic, b"command".to_vec()),
            )
            .unwrap();
        assert_eq!(committed.index, 1);
        assert!(consensus.finish_pending_rpcs(Duration::from_secs(1)));
        let proof = match consensus
            .inspect_certified_decision_at(1, LogHash::ZERO)
            .unwrap()
        {
            CertifiedDecisionInspection::Committed(decision) => decision.proof,
            other => panic!("expected committed decision, got {other:?}"),
        };

        let inspector =
            TcpPostcardRecorderClient::new(first_address, "node-1", "node-1", "peer-token-1", 1)
                .unwrap();
        let summary = inspector.inspect_record_summary(1).unwrap().unwrap();
        assert_eq!(summary.decided, None);
        assert_eq!(summary.step, 4);
        assert_eq!(summary.first_current.as_ref(), Some(proof.proposal()));
        assert_eq!(summary.aggregate_prior, None);
        assert_eq!(inspector.inspect_decision_proof(1).unwrap(), None);

        // The explicit proof-install transport remains for Phase2 and
        // configuration transitions; this ordinary FastPath elides it.
        inspector
            .install_decision_proof(proof.clone(), &install_membership)
            .unwrap();
        assert_eq!(inspector.inspect_decision_proof(1).unwrap(), Some(proof));
    })
    .await
    .unwrap();
    for server in servers {
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn recorder_tcp_rejects_wrong_peer_generation_and_identity() {
    let root = tempfile::tempdir().unwrap();
    let recorder = RecorderFileStore::new_with_id(
        root.path().join("recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_recorder_tcp(
        listener,
        recorder,
        peers(),
        7,
        std::future::pending(),
    ));

    tokio::task::spawn_blocking(move || {
        for (node_id, token, generation) in [
            ("node-9", "peer-token-2", 7),
            ("node-2", "wrong-token", 7),
            ("node-2", "peer-token-2", 6),
        ] {
            let client =
                TcpPostcardRecorderClient::new(address, "node-1", node_id, token, generation)
                    .unwrap();
            assert!(matches!(client.recorder_id(), Err(Error::Io(_))));
        }
    })
    .await
    .unwrap();
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn recorder_tcp_reconnects_after_a_broken_pooled_connection() {
    let root = tempfile::tempdir().unwrap();
    let recorder = RecorderFileStore::new_with_id(
        root.path().join("recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let first_server = tokio::spawn(serve_recorder_tcp(
        listener,
        recorder.clone(),
        peers(),
        1,
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    let client = Arc::new(
        TcpPostcardRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 1).unwrap(),
    );
    let first_client = client.clone();
    tokio::task::spawn_blocking(move || assert_eq!(first_client.recorder_id().unwrap(), "node-1"))
        .await
        .unwrap();
    shutdown_tx.send(()).unwrap();
    first_server.await.unwrap().unwrap();

    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let second_server = tokio::spawn(serve_recorder_tcp(
        listener,
        recorder,
        peers(),
        1,
        std::future::pending(),
    ));
    tokio::task::spawn_blocking(move || {
        assert!(matches!(client.recorder_id(), Err(Error::Io(_))));
        assert_eq!(client.recorder_id().unwrap(), "node-1");
    })
    .await
    .unwrap();
    second_server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn recorder_tcp_shutdown_waits_for_an_admitted_mutation() {
    let root = tempfile::tempdir().unwrap();
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let config_digest = membership.digest();
    let inner = RecorderFileStore::new_with_membership(
        root.path().join("recorder"),
        "node-1",
        "rhiza:sql:cluster-a",
        1,
        1,
        membership,
    )
    .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let recorder = BlockingStore {
        inner: inner.clone(),
        started: started_tx,
        release: Arc::clone(&release),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve_recorder_tcp(
        listener,
        recorder,
        peers(),
        1,
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    let client =
        TcpPostcardRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 1).unwrap();
    let command = StoredCommand::new(EntryType::Command, b"shutdown-command".to_vec());
    let command_hash = command.hash();
    let call = tokio::task::spawn_blocking(move || {
        client.store_command_for(
            "rhiza:sql:cluster-a".into(),
            1,
            1,
            config_digest,
            command_hash,
            command,
        )
    });
    tokio::task::spawn_blocking(move || started_rx.recv().unwrap())
        .await
        .unwrap();

    shutdown_tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!server.is_finished());
    let (released, ready) = &*release;
    *released.lock().unwrap() = true;
    ready.notify_all();

    assert!(call.await.unwrap().is_err());
    server.await.unwrap().unwrap();
    assert!(inner
        .fetch_command_for(
            "rhiza:sql:cluster-a".into(),
            1,
            1,
            config_digest,
            command_hash,
        )
        .unwrap()
        .is_some());
}
