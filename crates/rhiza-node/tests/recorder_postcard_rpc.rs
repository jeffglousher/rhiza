#![cfg(feature = "recorder-postcard-rpc")]

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Barrier, Condvar, Mutex,
    },
    thread,
    time::Duration,
};

use rhiza_core::{EntryType, LogHash, StoredCommand};
use rhiza_node::{
    serve_recorder_postcard_rpc, serve_recorder_postcard_rpc_tls, serve_recorder_tcp,
    serve_recorder_tcp_tls, PeerConfig, RecorderPostcardRpcTlsClientConfig,
    RecorderPostcardRpcTlsServerConfig, RecorderTlsClientConfig, RecorderTlsServerConfig,
    TcpPostcardRecorderClient, TcpPostcardRpcRecorderClient,
};
use rhiza_quepaxa::{
    AcceptedValue, DecisionProof, Error, Membership, Proposal, ProposalPriority,
    ReadFenceObservation, ReadFenceRequest, ReadFenceSlotState, RecordRequest, RecordSummary,
    RecorderRpc, RejectReason,
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

fn proposal(command: &StoredCommand) -> Proposal {
    Proposal::new(
        ProposalPriority::MAX,
        "node-1",
        1,
        AcceptedValue::from_command("rhiza:sql:cluster-a", 4, 1, 1, LogHash::ZERO, command),
    )
}

fn summary(slot: u64, digest: LogHash, proposal: Proposal) -> RecordSummary {
    RecordSummary {
        recorder_id: "node-1".into(),
        slot,
        config_id: 1,
        config_digest: digest,
        step: 4,
        first_current: Some(proposal),
        aggregate_prior: None,
        decided: None,
    }
}

fn record_request(slot: u64) -> RecordRequest {
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let command = StoredCommand::new(EntryType::Command, format!("command-{slot}").into_bytes());
    RecordRequest {
        cluster_id: "rhiza:sql:cluster-a".into(),
        epoch: 1,
        config_id: 1,
        config_digest: membership.digest(),
        slot,
        step: 4,
        proposal: proposal(&command),
        command: Some(command),
    }
}

fn decision_proof(proposer_id: &str, slot: u64) -> DecisionProof {
    let mut request = record_request(slot);
    request.proposal.proposer_id = proposer_id.into();
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

fn tls_material(name: &str) -> (String, String) {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec![name.to_string()]).unwrap();
    (cert.pem(), signing_key.serialize_pem())
}

#[derive(Default)]
struct RecorderState {
    commands: HashMap<LogHash, StoredCommand>,
    proof: Option<DecisionProof>,
    summaries: HashMap<u64, RecordSummary>,
}

#[derive(Clone, Default)]
struct ProbeRecorder {
    state: Arc<Mutex<RecorderState>>,
}

impl RecorderRpc for ProbeRecorder {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn store_command_for(
        &self,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        self.state
            .lock()
            .unwrap()
            .commands
            .insert(command_hash, command);
        Ok(())
    }

    fn fetch_command_for(
        &self,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .commands
            .get(&command_hash)
            .cloned())
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        let result = summary(request.slot, request.config_digest, request.proposal);
        self.state
            .lock()
            .unwrap()
            .summaries
            .insert(request.slot, result.clone());
        Ok(result)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        _membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        self.state.lock().unwrap().proof = Some(proof);
        Ok(())
    }

    fn inspect_decision_proof(&self, _slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        Ok(self.state.lock().unwrap().proof.clone())
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        Ok(self.state.lock().unwrap().summaries.get(&slot).cloned())
    }

    fn supports_context_read_fence(&self) -> bool {
        true
    }

    fn observe_read_fence(
        &self,
        request: ReadFenceRequest,
    ) -> rhiza_quepaxa::Result<ReadFenceObservation> {
        let state = self.state.lock().unwrap();
        let max_head = state.summaries.keys().copied().max();
        let summary = state.summaries.get(&request.slot).cloned().map(Box::new);
        let slot_state =
            if summary.is_none() && max_head.is_none_or(|max_head| max_head < request.slot) {
                ReadFenceSlotState::Empty
            } else {
                ReadFenceSlotState::Occupied { summary }
            };
        Ok(ReadFenceObservation {
            recorder_id: "node-1".into(),
            cluster_id: request.cluster_id,
            epoch: request.epoch,
            config_id: request.config_id,
            config_digest: request.config_digest,
            slot: request.slot,
            max_head,
            slot_state,
        })
    }
}

async fn server<R: RecorderRpc + Clone + Send + Sync + 'static>(
    recorder: R,
) -> (
    std::net::SocketAddr,
    tokio::task::JoinHandle<Result<(), String>>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(serve_recorder_postcard_rpc(
        listener,
        recorder,
        peers(),
        7,
        std::future::pending(),
    ));
    (address, server)
}

fn client(address: std::net::SocketAddr) -> TcpPostcardRpcRecorderClient {
    TcpPostcardRpcRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_accepts_member_relay_and_rejects_non_member_without_backend_call() {
    let recorder = ProbeRecorder::default();
    let state = Arc::clone(&recorder.state);
    let (address, server) = server(recorder).await;

    tokio::task::spawn_blocking(move || {
        let client = client(address);
        assert_eq!(client.record(record_request(1)).unwrap().slot, 1);
        let mut foreign = record_request(2);
        foreign.proposal.proposer_id = "node-9".into();
        assert!(matches!(
            client.record(foreign),
            Err(Error::Rejected(RejectReason::InvalidRequest))
        ));
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let proof = decision_proof("node-1", 3);
        client
            .install_decision_proof(proof.clone(), &membership)
            .unwrap();
        assert!(matches!(
            client.install_decision_proof(decision_proof("node-9", 4), &membership),
            Err(Error::Rejected(RejectReason::InvalidRequest))
        ));
        assert_eq!(client.recorder_id().unwrap(), "node-1");
    })
    .await
    .unwrap();

    assert_eq!(state.lock().unwrap().summaries.len(), 1);
    assert_eq!(
        state
            .lock()
            .unwrap()
            .proof
            .as_ref()
            .unwrap()
            .proposal()
            .proposer_id,
        "node-1"
    );
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_round_trips_all_eight_recorder_operations() {
    let recorder = ProbeRecorder::default();
    let (address, server) = server(recorder).await;
    let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
    let digest = membership.digest();
    let command = StoredCommand::new(EntryType::Command, b"command".to_vec());
    let command_hash = command.hash();
    let proposal = proposal(&command);
    let request = RecordRequest {
        cluster_id: "rhiza:sql:cluster-a".into(),
        epoch: 1,
        config_id: 1,
        config_digest: digest,
        slot: 4,
        step: 4,
        proposal: proposal.clone(),
        command: Some(command.clone()),
    };
    let proof = DecisionProof::FastPath {
        cluster_id: "rhiza:sql:cluster-a".into(),
        slot: 4,
        epoch: 1,
        config_id: 1,
        config_digest: digest,
        proposal,
        summaries: Vec::new(),
    };

    tokio::task::spawn_blocking(move || {
        let client = client(address);
        assert_eq!(client.recorder_id().unwrap(), "node-1");
        client
            .store_command_for(
                "rhiza:sql:cluster-a".into(),
                1,
                1,
                digest,
                command_hash,
                command.clone(),
            )
            .unwrap();
        assert_eq!(
            client
                .fetch_command_for("rhiza:sql:cluster-a".into(), 1, 1, digest, command_hash,)
                .unwrap(),
            Some(command)
        );
        let recorded = client.record(request).unwrap();
        assert_eq!(recorded.slot, 4);
        client
            .install_decision_proof(proof.clone(), &membership)
            .unwrap();
        assert_eq!(client.inspect_decision_proof(4).unwrap(), Some(proof));
        assert_eq!(client.inspect_record_summary(4).unwrap(), Some(recorded));
        assert!(matches!(
            client
                .observe_read_fence(ReadFenceRequest {
                    cluster_id: "rhiza:sql:cluster-a".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: digest,
                    slot: 4,
                })
                .unwrap()
                .slot_state,
            ReadFenceSlotState::Occupied {
                summary: Some(summary)
            } if summary.slot == 4
        ));
    })
    .await
    .unwrap();
    server.abort();
}

#[derive(Clone)]
struct ReorderingRecorder {
    first_started: mpsc::Sender<()>,
}

impl RecorderRpc for ReorderingRecorder {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        if slot == 1 {
            let _ = self.first_started.send(());
            thread::sleep(Duration::from_millis(150));
        }
        Ok(None)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_matches_two_out_of_order_responses_on_one_session() {
    let (started_tx, started_rx) = mpsc::channel();
    let (address, server) = server(ReorderingRecorder {
        first_started: started_tx,
    })
    .await;
    let client = Arc::new(client(address));
    let (done_tx, done_rx) = mpsc::channel();
    let first = Arc::clone(&client);
    let first_done = done_tx.clone();
    let first_call = thread::spawn(move || {
        first.inspect_record_summary(1).unwrap();
        first_done.send(1).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let second_call = thread::spawn(move || {
        client.inspect_record_summary(2).unwrap();
        done_tx.send(2).unwrap();
    });

    assert_eq!(done_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 2);
    assert_eq!(done_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
    first_call.join().unwrap();
    second_call.join().unwrap();
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_c32_control_burst_queues_without_bridge_overload() {
    let (address, server) = server(ProbeRecorder::default()).await;
    let client = Arc::new(client(address));
    let start = Arc::new(Barrier::new(33));
    let calls = (0..32)
        .map(|slot| {
            let client = Arc::clone(&client);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                client.inspect_record_summary(slot)
            })
        })
        .collect::<Vec<_>>();
    start.wait();

    let errors = calls
        .into_iter()
        .filter_map(|call| call.join().unwrap().err())
        .collect::<Vec<_>>();
    assert!(errors.is_empty(), "c32 burst errors: {errors:?}");
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_preserves_frames_during_sustained_c4_multiplexing() {
    let (address, server) = server(ProbeRecorder::default()).await;
    let client = Arc::new(client(address));
    let start = Arc::new(Barrier::new(5));
    let calls = (0..4)
        .map(|worker| {
            let client = Arc::clone(&client);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                (worker..10_000)
                    .step_by(4)
                    .find_map(|slot| client.inspect_record_summary(slot).err())
            })
        })
        .collect::<Vec<_>>();
    start.wait();

    let errors = calls
        .into_iter()
        .filter_map(|call| call.join().unwrap())
        .collect::<Vec<_>>();
    assert!(errors.is_empty(), "sustained c4 errors: {errors:?}");
    server.abort();
}

#[derive(Clone)]
struct BlockingRecord {
    started: mpsc::Sender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl RecorderRpc for BlockingRecord {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        self.started.send(()).unwrap();
        let (released, ready) = &*self.release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        Ok(summary(
            request.slot,
            request.config_digest,
            request.proposal,
        ))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_control_lane_progresses_while_consensus_lane_is_blocked() {
    let (started_tx, started_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let (address, server) = server(BlockingRecord {
        started: started_tx,
        release: Arc::clone(&release),
    })
    .await;
    let client = Arc::new(client(address));
    let consensus = Arc::clone(&client);
    let call = thread::spawn(move || consensus.record(record_request(9)));
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    assert_eq!(client.recorder_id().unwrap(), "node-1");
    let (released, ready) = &*release;
    *released.lock().unwrap() = true;
    ready.notify_all();
    assert_eq!(call.join().unwrap().unwrap().slot, 9);
    server.abort();
}

#[derive(Clone)]
struct BlockingStoreOnce {
    started: mpsc::Sender<()>,
    release: Arc<(Mutex<bool>, Condvar)>,
    stores: Arc<AtomicUsize>,
}

impl RecorderRpc for BlockingStoreOnce {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn store_command_for(
        &self,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        _command_hash: LogHash,
        _command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        self.started.send(()).unwrap();
        let (released, ready) = &*self.release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        self.stores.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_control_lane_progresses_while_command_store_is_blocked() {
    let (started_tx, started_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let store_count = Arc::new(AtomicUsize::new(0));
    let (address, server) = server(BlockingStoreOnce {
        started: started_tx,
        release: Arc::clone(&release),
        stores: Arc::clone(&store_count),
    })
    .await;
    let client = Arc::new(client(address));
    let stores = (0..8)
        .map(|index| {
            let client = Arc::clone(&client);
            thread::spawn(move || {
                let command = StoredCommand::new(
                    EntryType::Command,
                    format!("blocked-store-{index}").into_bytes(),
                );
                client.store_command_for(
                    "rhiza:sql:cluster-a".into(),
                    1,
                    1,
                    Membership::new(["node-1", "node-2", "node-3"])
                        .unwrap()
                        .digest(),
                    command.hash(),
                    command,
                )
            })
        })
        .collect::<Vec<_>>();
    for _ in 0..stores.len() {
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }

    let identity = client.recorder_id();
    let (released, ready) = &*release;
    *released.lock().unwrap() = true;
    ready.notify_all();
    assert_eq!(identity.unwrap(), "node-1");
    for store in stores {
        assert!(store.join().unwrap().is_ok());
    }
    assert_eq!(store_count.load(Ordering::SeqCst), 8);
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_does_not_replay_a_mutation_after_session_failure_and_later_reconnects() {
    let (started_tx, started_rx) = mpsc::channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let stores = Arc::new(AtomicUsize::new(0));
    let recorder = BlockingStoreOnce {
        started: started_tx,
        release: Arc::clone(&release),
        stores: Arc::clone(&stores),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let first_server = tokio::spawn(serve_recorder_postcard_rpc(
        listener,
        recorder.clone(),
        peers(),
        7,
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    let client = Arc::new(client(address));
    let mutation_client = Arc::clone(&client);
    let command = StoredCommand::new(EntryType::Command, b"mutate-once".to_vec());
    let command_hash = command.hash();
    let mutation = thread::spawn(move || {
        mutation_client.store_command_for(
            "rhiza:sql:cluster-a".into(),
            1,
            1,
            Membership::new(["node-1", "node-2", "node-3"])
                .unwrap()
                .digest(),
            command_hash,
            command,
        )
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    shutdown_tx.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!first_server.is_finished());
    let (released, ready) = &*release;
    *released.lock().unwrap() = true;
    ready.notify_all();
    assert!(mutation.join().unwrap().is_err());
    first_server.await.unwrap().unwrap();
    assert_eq!(stores.load(Ordering::SeqCst), 1);

    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let second_server = tokio::spawn(serve_recorder_postcard_rpc(
        listener,
        ProbeRecorder::default(),
        peers(),
        7,
        std::future::pending(),
    ));
    assert_eq!(client.recorder_id().unwrap(), "node-1");
    assert_eq!(stores.load(Ordering::SeqCst), 1);
    second_server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_tls_round_trips_and_protocol_fences_reject_mismatches() {
    let (cert_pem, key_pem) = tls_material("recorder.test");

    let rpc_tls_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc_tls_address = rpc_tls_listener.local_addr().unwrap();
    let rpc_tls_server = tokio::spawn(serve_recorder_postcard_rpc_tls(
        rpc_tls_listener,
        ProbeRecorder::default(),
        peers(),
        7,
        RecorderPostcardRpcTlsServerConfig::from_pem(cert_pem.as_bytes(), key_pem.as_bytes())
            .unwrap(),
        std::future::pending(),
    ));
    let rpc_tls =
        RecorderPostcardRpcTlsClientConfig::from_ca_pem(cert_pem.as_bytes(), "recorder.test")
            .unwrap();
    let matching = TcpPostcardRpcRecorderClient::new_tls(
        rpc_tls_address,
        "node-1",
        "node-2",
        "peer-token-2",
        7,
        rpc_tls.clone(),
    )
    .unwrap();
    assert_eq!(matching.recorder_id().unwrap(), "node-1");
    let framed_tls =
        RecorderTlsClientConfig::from_ca_pem(cert_pem.as_bytes(), "recorder.test").unwrap();
    let framed_to_rpc = TcpPostcardRecorderClient::new_tls(
        rpc_tls_address,
        "node-1",
        "node-2",
        "peer-token-2",
        7,
        framed_tls,
    )
    .unwrap();
    assert!(framed_to_rpc.recorder_id().is_err());
    assert!(client(rpc_tls_address).recorder_id().is_err());

    let framed_tls_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let framed_tls_address = framed_tls_listener.local_addr().unwrap();
    let framed_tls_server = tokio::spawn(serve_recorder_tcp_tls(
        framed_tls_listener,
        ProbeRecorder::default(),
        peers(),
        7,
        RecorderTlsServerConfig::from_pem(cert_pem.as_bytes(), key_pem.as_bytes()).unwrap(),
        std::future::pending(),
    ));
    let rpc_to_framed = TcpPostcardRpcRecorderClient::new_tls(
        framed_tls_address,
        "node-1",
        "node-2",
        "peer-token-2",
        7,
        rpc_tls.clone(),
    )
    .unwrap();
    assert!(rpc_to_framed.recorder_id().is_err());

    let rpc_plain_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc_plain_address = rpc_plain_listener.local_addr().unwrap();
    let rpc_plain_server = tokio::spawn(serve_recorder_postcard_rpc(
        rpc_plain_listener,
        ProbeRecorder::default(),
        peers(),
        7,
        std::future::pending(),
    ));
    let framed_plain =
        TcpPostcardRecorderClient::new(rpc_plain_address, "node-1", "node-2", "peer-token-2", 7)
            .unwrap();
    assert!(framed_plain.recorder_id().is_err());
    let tls_to_plain = TcpPostcardRpcRecorderClient::new_tls(
        rpc_plain_address,
        "node-1",
        "node-2",
        "peer-token-2",
        7,
        rpc_tls,
    )
    .unwrap();
    assert!(tls_to_plain.recorder_id().is_err());

    let framed_plain_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let framed_plain_address = framed_plain_listener.local_addr().unwrap();
    let framed_plain_server = tokio::spawn(serve_recorder_tcp(
        framed_plain_listener,
        ProbeRecorder::default(),
        peers(),
        7,
        std::future::pending(),
    ));
    assert!(client(framed_plain_address).recorder_id().is_err());

    rpc_tls_server.abort();
    framed_tls_server.abort();
    rpc_plain_server.abort();
    framed_plain_server.abort();
}
