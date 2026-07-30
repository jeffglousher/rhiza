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
    serve_recorder_tcp_tls, PeerConfig, RecorderIngressExit, RecorderIngressLifecycle,
    RecorderPostcardRpcTlsClientConfig, RecorderPostcardRpcTlsServerConfig,
    RecorderTlsClientConfig, RecorderTlsServerConfig, TcpPostcardRecorderClient,
    TcpPostcardRpcRecorderClient,
};
use rhiza_quepaxa::{
    AcceptedValue, DecisionProof, Error, Membership, Proposal, ProposalPriority,
    ReadFenceObservation, ReadFenceRequest, ReadFenceSlotState, RecordRequest, RecordSummary,
    RecorderRpc, RecorderRpcContext, RejectReason,
};

fn context() -> RecorderRpcContext {
    RecorderRpcContext::default_timeout()
}

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

struct TestIngressControl {
    shutdown: tokio::sync::watch::Sender<bool>,
    _force: tokio::sync::watch::Sender<bool>,
    _started: tokio::sync::oneshot::Receiver<()>,
    _listener_dropped: tokio::sync::oneshot::Receiver<()>,
}

fn ingress_lifecycle() -> (TestIngressControl, RecorderIngressLifecycle) {
    let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let (force, force_rx) = tokio::sync::watch::channel(false);
    let (started, started_rx) = tokio::sync::oneshot::channel();
    let (listener_dropped, listener_dropped_rx) = tokio::sync::oneshot::channel();
    (
        TestIngressControl {
            shutdown,
            _force: force,
            _started: started_rx,
            _listener_dropped: listener_dropped_rx,
        },
        RecorderIngressLifecycle::new(shutdown_rx, force_rx, started, listener_dropped),
    )
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
    fn recorder_id(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn store_command_for(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
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
        _context: &rhiza_quepaxa::RecorderRpcContext,
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

    fn record(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        request: RecordRequest,
    ) -> rhiza_quepaxa::Result<RecordSummary> {
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
        _context: &rhiza_quepaxa::RecorderRpcContext,
        proof: DecisionProof,
        _membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        self.state.lock().unwrap().proof = Some(proof);
        Ok(())
    }

    fn inspect_decision_proof(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        _slot: u64,
    ) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        Ok(self.state.lock().unwrap().proof.clone())
    }

    fn inspect_record_summary(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        Ok(self.state.lock().unwrap().summaries.get(&slot).cloned())
    }

    fn supports_context_read_fence(&self) -> bool {
        true
    }

    fn observe_read_fence(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
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

#[derive(Clone)]
struct SafetyErrorRecorder {
    error: Error,
}

impl RecorderRpc for SafetyErrorRecorder {
    fn recorder_id(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn fetch_command_for(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        _command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        Err(self.error.clone())
    }
}

#[derive(Clone, Default)]
struct PanicRecorder {
    mutation_started: Arc<AtomicUsize>,
}

impl RecorderRpc for PanicRecorder {
    fn recorder_id(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn fetch_command_for(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        _cluster_id: String,
        _epoch: u64,
        _config_id: u64,
        _config_digest: LogHash,
        _command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        panic!("injected read-only recorder panic")
    }

    fn record(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        _request: RecordRequest,
    ) -> rhiza_quepaxa::Result<RecordSummary> {
        self.mutation_started.fetch_add(1, Ordering::SeqCst);
        panic!("injected mutating recorder panic")
    }
}

async fn server<R: RecorderRpc + Clone + Send + Sync + 'static>(
    recorder: R,
) -> (
    std::net::SocketAddr,
    TestIngressControl,
    tokio::task::JoinHandle<RecorderIngressExit>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (control, lifecycle) = ingress_lifecycle();
    let server = tokio::spawn(serve_recorder_postcard_rpc(
        listener,
        recorder,
        peers(),
        7,
        lifecycle,
    ));
    (address, control, server)
}

fn client(address: std::net::SocketAddr) -> TcpPostcardRpcRecorderClient {
    TcpPostcardRpcRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_accepts_member_relay_and_rejects_non_member_without_backend_call() {
    let recorder = ProbeRecorder::default();
    let state = Arc::clone(&recorder.state);
    let (address, _control, server) = server(recorder).await;

    tokio::task::spawn_blocking(move || {
        let client = client(address);
        assert_eq!(
            client.record(&context(), record_request(1)).unwrap().slot,
            1
        );
        let mut foreign = record_request(2);
        foreign.proposal.proposer_id = "node-9".into();
        assert!(matches!(
            client.record(&context(), foreign),
            Err(Error::Rejected(RejectReason::InvalidRequest))
        ));
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let proof = decision_proof("node-1", 3);
        client
            .install_decision_proof(&context(), proof.clone(), &membership)
            .unwrap();
        assert!(matches!(
            client.install_decision_proof(&context(), decision_proof("node-9", 4), &membership),
            Err(Error::Rejected(RejectReason::InvalidRequest))
        ));
        assert_eq!(client.recorder_id(&context()).unwrap(), "node-1");
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
async fn postcard_rpc_round_trips_safety_errors_exactly() {
    let chain = Error::ChainConflict {
        slot: 9,
        expected_prev_hash: LogHash::digest(&[b"expected"]),
        actual_prev_hash: LogHash::digest(&[b"actual"]),
    };
    for expected in [chain.clone(), Error::ConflictingCertificates] {
        let (address, _control, server) = server(SafetyErrorRecorder {
            error: expected.clone(),
        })
        .await;
        let actual = tokio::task::spawn_blocking(move || {
            client(address).fetch_command_for(
                &context(),
                "rhiza:sql:cluster-a".into(),
                1,
                1,
                LogHash::ZERO,
                LogHash::ZERO,
            )
        })
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(actual, expected);
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_panic_keeps_mutation_ambiguous_and_read_definite() {
    let recorder = PanicRecorder::default();
    let mutation_started = Arc::clone(&recorder.mutation_started);
    let (address, _control, server) = server(recorder).await;
    let (mutation, read) = tokio::task::spawn_blocking(move || {
        let client = client(address);
        let context = context();
        let mutation = client.record(&context, record_request(11));
        let read = client.fetch_command_for(
            &context,
            "rhiza:sql:cluster-a".into(),
            1,
            1,
            LogHash::ZERO,
            LogHash::ZERO,
        );
        (mutation, read)
    })
    .await
    .unwrap();

    assert_eq!(mutation_started.load(Ordering::SeqCst), 1);
    assert!(matches!(mutation, Err(Error::UnknownOutcome)));
    assert!(matches!(read, Err(Error::ProposeFailed)));
    server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_round_trips_all_eight_recorder_operations() {
    let recorder = ProbeRecorder::default();
    let (address, _control, server) = server(recorder).await;
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
        assert_eq!(client.recorder_id(&context()).unwrap(), "node-1");
        client
            .store_command_for(
                &context(),
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
                .fetch_command_for(
                    &context(),
                    "rhiza:sql:cluster-a".into(),
                    1,
                    1,
                    digest,
                    command_hash,
                )
                .unwrap(),
            Some(command)
        );
        let recorded = client.record(&context(), request).unwrap();
        assert_eq!(recorded.slot, 4);
        client
            .install_decision_proof(&context(), proof.clone(), &membership)
            .unwrap();
        assert_eq!(
            client.inspect_decision_proof(&context(), 4).unwrap(),
            Some(proof)
        );
        assert_eq!(
            client.inspect_record_summary(&context(), 4).unwrap(),
            Some(recorded)
        );
        assert!(matches!(
            client
                .observe_read_fence(&context(), ReadFenceRequest {
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
    fn recorder_id(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn inspect_record_summary(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
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
    let (address, _control, server) = server(ReorderingRecorder {
        first_started: started_tx,
    })
    .await;
    let client = Arc::new(client(address));
    let (done_tx, done_rx) = mpsc::channel();
    let first = Arc::clone(&client);
    let first_done = done_tx.clone();
    let first_call = thread::spawn(move || {
        first.inspect_record_summary(&context(), 1).unwrap();
        first_done.send(1).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let second_call = thread::spawn(move || {
        client.inspect_record_summary(&context(), 2).unwrap();
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
    let (address, _control, server) = server(ProbeRecorder::default()).await;
    let client = Arc::new(client(address));
    let start = Arc::new(Barrier::new(33));
    let calls = (0..32)
        .map(|slot| {
            let client = Arc::clone(&client);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                start.wait();
                client.inspect_record_summary(&context(), slot)
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
    let (address, _control, server) = server(ProbeRecorder::default()).await;
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
                    .find_map(|slot| client.inspect_record_summary(&context(), slot).err())
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
    fn recorder_id(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn record(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        request: RecordRequest,
    ) -> rhiza_quepaxa::Result<RecordSummary> {
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
    let (address, _control, server) = server(BlockingRecord {
        started: started_tx,
        release: Arc::clone(&release),
    })
    .await;
    let client = Arc::new(client(address));
    let consensus = Arc::clone(&client);
    let call = thread::spawn(move || consensus.record(&context(), record_request(9)));
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    assert_eq!(client.recorder_id(&context()).unwrap(), "node-1");
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
    fn recorder_id(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        Ok("node-1".into())
    }

    fn store_command_for(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
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
    let (address, _control, server) = server(BlockingStoreOnce {
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
                    &context(),
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

    let identity = client.recorder_id(&context());
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
    let (first_control, first_lifecycle) = ingress_lifecycle();
    let first_server = tokio::spawn(serve_recorder_postcard_rpc(
        listener,
        recorder.clone(),
        peers(),
        7,
        first_lifecycle,
    ));
    let client = Arc::new(client(address));
    let mutation_client = Arc::clone(&client);
    let command = StoredCommand::new(EntryType::Command, b"mutate-once".to_vec());
    let command_hash = command.hash();
    let mutation = thread::spawn(move || {
        mutation_client.store_command_for(
            &context(),
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
    first_control._force.send_replace(true);
    let first_exit = tokio::time::timeout(Duration::from_secs(1), first_server)
        .await
        .expect("forced postcard session did not exit")
        .unwrap();
    assert!(first_exit.result.is_ok());
    assert_eq!(
        first_exit.tasks,
        rhiza_node::RecorderTaskDisposition::Uncertain
    );
    let (released, ready) = &*release;
    *released.lock().unwrap() = true;
    ready.notify_all();
    assert!(mutation.join().unwrap().is_err());
    assert_eq!(stores.load(Ordering::SeqCst), 1);

    let listener = tokio::net::TcpListener::bind(address).await.unwrap();
    let (_second_control, second_lifecycle) = ingress_lifecycle();
    let second_server = tokio::spawn(serve_recorder_postcard_rpc(
        listener,
        ProbeRecorder::default(),
        peers(),
        7,
        second_lifecycle,
    ));
    assert_eq!(client.recorder_id(&context()).unwrap(), "node-1");
    assert_eq!(stores.load(Ordering::SeqCst), 1);
    second_server.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_tls_force_receipts_listener_and_reaps_a_stalled_handshake() {
    use tokio::io::AsyncReadExt;

    let (cert_pem, key_pem) = tls_material("recorder.test");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (control, lifecycle) = ingress_lifecycle();
    let TestIngressControl {
        shutdown: _shutdown,
        _force: force,
        _started: started,
        _listener_dropped: listener_dropped,
    } = control;
    let server = tokio::spawn(serve_recorder_postcard_rpc_tls(
        listener,
        ProbeRecorder::default(),
        peers(),
        7,
        RecorderPostcardRpcTlsServerConfig::from_pem(cert_pem.as_bytes(), key_pem.as_bytes())
            .unwrap(),
        lifecycle,
    ));
    started.await.unwrap();
    let mut stalled = tokio::net::TcpStream::connect(address).await.unwrap();

    force.send_replace(true);
    tokio::time::timeout(Duration::from_secs(1), listener_dropped)
        .await
        .expect("postcard-rpc TLS listener receipt timed out")
        .unwrap();
    let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
    drop(rebound);
    let exit = tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("stalled postcard-rpc TLS handshake was not force-reaped")
        .unwrap();
    assert!(exit.result.is_ok());
    assert_eq!(exit.tasks, rhiza_node::RecorderTaskDisposition::Uncertain);
    let mut closed = [0_u8; 1];
    match stalled.read(&mut closed).await {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) => {}
        result => panic!("stalled postcard-rpc TLS peer survived force: {result:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn postcard_rpc_tls_round_trips_and_protocol_fences_reject_mismatches() {
    let (cert_pem, key_pem) = tls_material("recorder.test");

    let rpc_tls_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc_tls_address = rpc_tls_listener.local_addr().unwrap();
    let (_rpc_tls_control, rpc_tls_lifecycle) = ingress_lifecycle();
    let rpc_tls_server = tokio::spawn(serve_recorder_postcard_rpc_tls(
        rpc_tls_listener,
        ProbeRecorder::default(),
        peers(),
        7,
        RecorderPostcardRpcTlsServerConfig::from_pem(cert_pem.as_bytes(), key_pem.as_bytes())
            .unwrap(),
        rpc_tls_lifecycle,
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
    assert_eq!(matching.recorder_id(&context()).unwrap(), "node-1");
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
    assert!(framed_to_rpc.recorder_id(&context()).is_err());
    assert!(client(rpc_tls_address).recorder_id(&context()).is_err());

    let framed_tls_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let framed_tls_address = framed_tls_listener.local_addr().unwrap();
    let (_framed_tls_control, framed_tls_lifecycle) = ingress_lifecycle();
    let framed_tls_server = tokio::spawn(serve_recorder_tcp_tls(
        framed_tls_listener,
        ProbeRecorder::default(),
        peers(),
        7,
        RecorderTlsServerConfig::from_pem(cert_pem.as_bytes(), key_pem.as_bytes()).unwrap(),
        framed_tls_lifecycle,
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
    assert!(rpc_to_framed.recorder_id(&context()).is_err());

    let rpc_plain_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rpc_plain_address = rpc_plain_listener.local_addr().unwrap();
    let (_rpc_plain_control, rpc_plain_lifecycle) = ingress_lifecycle();
    let rpc_plain_server = tokio::spawn(serve_recorder_postcard_rpc(
        rpc_plain_listener,
        ProbeRecorder::default(),
        peers(),
        7,
        rpc_plain_lifecycle,
    ));
    let framed_plain =
        TcpPostcardRecorderClient::new(rpc_plain_address, "node-1", "node-2", "peer-token-2", 7)
            .unwrap();
    assert!(framed_plain.recorder_id(&context()).is_err());
    let tls_to_plain = TcpPostcardRpcRecorderClient::new_tls(
        rpc_plain_address,
        "node-1",
        "node-2",
        "peer-token-2",
        7,
        rpc_tls,
    )
    .unwrap();
    assert!(tls_to_plain.recorder_id(&context()).is_err());

    let framed_plain_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let framed_plain_address = framed_plain_listener.local_addr().unwrap();
    let (_framed_plain_control, framed_plain_lifecycle) = ingress_lifecycle();
    let framed_plain_server = tokio::spawn(serve_recorder_tcp(
        framed_plain_listener,
        ProbeRecorder::default(),
        peers(),
        7,
        framed_plain_lifecycle,
    ));
    assert!(client(framed_plain_address)
        .recorder_id(&context())
        .is_err());

    rpc_tls_server.abort();
    framed_tls_server.abort();
    rpc_plain_server.abort();
    framed_plain_server.abort();
}
