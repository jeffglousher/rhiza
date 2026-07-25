use std::{
    collections::{BTreeMap, HashMap},
    env,
    net::SocketAddr,
    process::{self, Command},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Barrier, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rhiza_core::{EntryType, LogHash, StoredCommand};
use rhiza_node::{
    serve_recorder_rkyv_tcp, serve_recorder_tcp, PeerConfig, TcpPostcardRecorderClient,
    TcpRkyvRecorderClient,
};
use rhiza_quepaxa::{
    AcceptedValue, DecisionProof, Error, Membership, Proposal, ProposalPriority,
    ReadFenceObservation, ReadFenceRequest, ReadFenceSlotState, RecordRequest, RecordSummary,
    RecorderRpc,
};
use serde::Serialize;
use tokio::sync::oneshot;

const RECORDER_ID: &str = "node-1";
const CLIENT_ID: &str = "node-2";
const CLIENT_TOKEN: &str = "peer-token-2";
const RECOVERY_GENERATION: u64 = 7;
const LENGTH_PREFIX_BYTES: usize = 4;
const MAX_DISTINCT_ERROR_MESSAGES: usize = 8;
const RECORDER_SERVER_OPERATION_CAP: usize = 32;
const POSTCARD_WIRE_VERSION: u16 = 3;
const RKYV_WIRE_VERSION: u16 = 6;
const PAYLOAD_SIZES: [usize; 3] = [0, 128, 4096];
// Production recorder calls allow 10 seconds; fail the benchmark-only rendezvous
// early enough that both client waiters can return and report the failed prewarm.
const PREWARM_GATE_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(clippy::enum_variant_names)]
enum Candidate {
    TcpPostcard,
    TcpRkyv,
}

impl Candidate {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "tcp-postcard" => Some(Self::TcpPostcard),
            "tcp-rkyv" => Some(Self::TcpRkyv),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::TcpPostcard => "tcp-postcard",
            Self::TcpRkyv => "tcp-rkyv",
        }
    }

    fn codec(self) -> &'static str {
        match self {
            Self::TcpPostcard => "postcard",
            Self::TcpRkyv => "rkyv",
        }
    }

    fn wire_version(self) -> u16 {
        match self {
            Self::TcpPostcard => POSTCARD_WIRE_VERSION,
            Self::TcpRkyv => RKYV_WIRE_VERSION,
        }
    }

    fn topology(self) -> &'static str {
        "one shared production client; exactly two pooled production connections per consensus/control lane; callers queue while both are busy"
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Workload {
    Identity,
    StoreCommand(usize),
    FetchCommand(usize),
    Record,
    RecordPayload(usize),
    InstallDecisionProof,
    InspectDecisionProof,
    InspectRecordSummary,
    ObserveReadFence,
}

impl Workload {
    const ALL: [Self; 14] = [
        Self::Identity,
        Self::StoreCommand(0),
        Self::StoreCommand(128),
        Self::StoreCommand(4096),
        Self::FetchCommand(0),
        Self::FetchCommand(128),
        Self::FetchCommand(4096),
        Self::RecordPayload(0),
        Self::RecordPayload(128),
        Self::RecordPayload(4096),
        Self::InstallDecisionProof,
        Self::InspectDecisionProof,
        Self::InspectRecordSummary,
        Self::ObserveReadFence,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::StoreCommand(_) => "store_command",
            Self::FetchCommand(_) => "fetch_command",
            Self::Record | Self::RecordPayload(_) => "record",
            Self::InstallDecisionProof => "install_decision_proof",
            Self::InspectDecisionProof => "inspect_decision_proof",
            Self::InspectRecordSummary => "inspect_record_summary",
            Self::ObserveReadFence => "observe_read_fence",
        }
    }

    fn cell_id(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::StoreCommand(0) => "store_command/payload-0",
            Self::StoreCommand(128) => "store_command/payload-128",
            Self::StoreCommand(4096) => "store_command/payload-4096",
            Self::FetchCommand(0) => "fetch_command/payload-0",
            Self::FetchCommand(128) => "fetch_command/payload-128",
            Self::FetchCommand(4096) => "fetch_command/payload-4096",
            Self::Record | Self::RecordPayload(0) => "record/payload-0",
            Self::RecordPayload(128) => "record/payload-128",
            Self::RecordPayload(4096) => "record/payload-4096",
            Self::InstallDecisionProof => "install_decision_proof",
            Self::InspectDecisionProof => "inspect_decision_proof",
            Self::InspectRecordSummary => "inspect_record_summary",
            Self::ObserveReadFence => "observe_read_fence",
            _ => unreachable!("unsupported benchmark payload"),
        }
    }

    fn payload_bytes(self) -> Option<usize> {
        match self {
            Self::StoreCommand(bytes) | Self::FetchCommand(bytes) | Self::RecordPayload(bytes) => {
                Some(bytes)
            }
            Self::Record => Some(0),
            _ => None,
        }
    }

    fn lane(self) -> &'static str {
        match self {
            Self::Record | Self::RecordPayload(_) | Self::InstallDecisionProof => "consensus",
            _ => "control",
        }
    }

    fn parse_cell_id(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|workload| workload.cell_id() == value)
    }
}

struct Config {
    warmup: usize,
    operations: usize,
    min_duration: Duration,
    concurrencies: Vec<usize>,
    candidates: Vec<Candidate>,
    candidate_order_offset: usize,
    workloads: Vec<Workload>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            warmup: 1_000,
            operations: 10_000,
            min_duration: Duration::from_millis(250),
            concurrencies: vec![1, 4, 32],
            candidates: vec![Candidate::TcpPostcard],
            candidate_order_offset: 0,
            workloads: Workload::ALL.to_vec(),
        }
    }
}

impl Config {
    fn parse_from(args: &[String]) -> Result<Self, String> {
        let mut config = Self::default();
        let mut index = 1;
        while index < args.len() {
            let flag = &args[index];
            if flag == "--help" || flag == "-h" {
                print_usage();
                process::exit(0);
            }
            let value = args
                .get(index + 1)
                .ok_or_else(|| format!("{flag} requires a value"))?;
            match flag.as_str() {
                "--warmup" => config.warmup = parse_positive(value, flag)?,
                "--operations" => config.operations = parse_positive(value, flag)?,
                "--min-duration-ms" => {
                    config.min_duration = Duration::from_millis(parse_positive(value, flag)? as u64)
                }
                "--concurrency" => config.concurrencies = parse_positive_list(value, flag)?,
                "--cells" => config.workloads = parse_cells(value)?,
                "--candidate-order-offset" => {
                    config.candidate_order_offset = value
                        .parse()
                        .map_err(|_| format!("{flag} requires a non-negative integer"))?
                }
                "--candidates" => {
                    config.candidates = value
                        .split(',')
                        .map(|name| {
                            Candidate::parse(name)
                                .ok_or_else(|| format!("unknown candidate {name:?}"))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    if config.candidates.is_empty() {
                        return Err("--candidates must not be empty".into());
                    }
                    if config.candidates.len() != 1 {
                        return Err(
                            "--candidates requires exactly one exclusive transport per raw run"
                                .into(),
                        );
                    }
                }
                _ => return Err(format!("unknown option {flag:?}")),
            }
            index += 2;
        }
        let offset = config.candidate_order_offset % config.candidates.len();
        config.candidates.rotate_left(offset);
        Ok(config)
    }
}

fn parse_positive(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{flag} requires a positive integer"))
}

fn parse_positive_list(value: &str, flag: &str) -> Result<Vec<usize>, String> {
    let values = value
        .split(',')
        .map(|value| parse_positive(value, flag))
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() {
        Err(format!("{flag} must not be empty"))
    } else {
        Ok(values)
    }
}

fn parse_cells(value: &str) -> Result<Vec<Workload>, String> {
    let mut workloads = Vec::new();
    for cell_id in value.split(',') {
        let workload = Workload::parse_cell_id(cell_id)
            .ok_or_else(|| format!("unknown or non-canonical cell {cell_id:?}"))?;
        if workloads.contains(&workload) {
            return Err(format!("duplicate cell {cell_id:?}"));
        }
        workloads.push(workload);
    }
    if workloads.is_empty() {
        return Err("--cells must not be empty".into());
    }
    Ok(workloads)
}

fn print_usage() {
    eprintln!(
        "Usage: rhiza-recorder-transport [--warmup N] [--operations MIN] \
         [--min-duration-ms N] [--concurrency N,N] [--cells ID,ID] \
         [--candidates NAME] [--candidate-order-offset N]\n\
         Candidates: tcp-postcard,tcp-rkyv"
    );
}

#[derive(Clone)]
struct DeterministicRecorder {
    membership: Membership,
    commands: Arc<Mutex<HashMap<LogHash, StoredCommand>>>,
    prewarm: Arc<PrewarmGates>,
}

impl Default for DeterministicRecorder {
    fn default() -> Self {
        let membership = membership();
        let commands = PAYLOAD_SIZES
            .into_iter()
            .map(command_for_size)
            .map(|command| (command.hash(), command))
            .collect();
        Self {
            membership,
            commands: Arc::new(Mutex::new(commands)),
            prewarm: Arc::new(PrewarmGates::default()),
        }
    }
}

impl RecorderRpc for DeterministicRecorder {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        Ok(RECORDER_ID.into())
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
        if command.hash() != command_hash {
            return Err(Error::Decode("benchmark command hash mismatch".into()));
        }
        self.commands.lock().unwrap().insert(command_hash, command);
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
        Ok(self.commands.lock().unwrap().get(&command_hash).cloned())
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        self.prewarm.consensus.wait();
        Ok(summary_for(
            request.slot,
            request.config_digest,
            request.proposal,
        ))
    }

    fn install_decision_proof(
        &self,
        _proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        if membership == &self.membership {
            Ok(())
        } else {
            Err(Error::Decode("benchmark membership mismatch".into()))
        }
    }

    fn inspect_decision_proof(&self, slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        Ok(Some(proof_for(slot)))
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        self.prewarm.control.wait();
        Ok(Some(summary_for(
            slot,
            self.membership.digest(),
            proposal_for(slot, 0),
        )))
    }

    fn supports_context_read_fence(&self) -> bool {
        true
    }

    fn observe_read_fence(
        &self,
        request: ReadFenceRequest,
    ) -> rhiza_quepaxa::Result<ReadFenceObservation> {
        Ok(ReadFenceObservation {
            recorder_id: RECORDER_ID.into(),
            cluster_id: request.cluster_id,
            epoch: request.epoch,
            config_id: request.config_id,
            config_digest: request.config_digest,
            slot: request.slot,
            max_head: None,
            slot_state: ReadFenceSlotState::Empty,
        })
    }
}

#[derive(Default)]
struct PrewarmGateState {
    armed: bool,
    arrived: usize,
}

#[derive(Default)]
struct PrewarmGate {
    state: Mutex<PrewarmGateState>,
    changed: Condvar,
    failures: AtomicUsize,
}

impl PrewarmGate {
    fn arm(&self) {
        let mut state = self.state.lock().unwrap();
        state.armed = true;
        state.arrived = 0;
    }

    fn wait(&self) {
        let mut state = self.state.lock().unwrap();
        if !state.armed {
            return;
        }
        state.arrived += 1;
        if state.arrived == 2 {
            state.armed = false;
            self.changed.notify_all();
            return;
        }
        while state.armed {
            let (next, timeout) = self
                .changed
                .wait_timeout(state, PREWARM_GATE_TIMEOUT)
                .unwrap();
            state = next;
            if timeout.timed_out() && state.armed {
                state.armed = false;
                self.failures.fetch_add(1, Ordering::Relaxed);
                self.changed.notify_all();
            }
        }
    }

    fn take_failures(&self) -> usize {
        self.failures.swap(0, Ordering::Relaxed)
    }
}

#[derive(Default)]
struct PrewarmGates {
    consensus: PrewarmGate,
    control: PrewarmGate,
}

impl PrewarmGates {
    fn arm(&self) {
        self.consensus.arm();
        self.control.arm();
    }

    fn take_failures(&self) -> usize {
        self.consensus.take_failures() + self.control.take_failures()
    }
}

fn membership() -> Membership {
    Membership::new(["node-1", "node-2", "node-3"]).unwrap()
}

fn command_for_size(payload_bytes: usize) -> StoredCommand {
    StoredCommand::new(EntryType::Command, vec![0x5a; payload_bytes])
}

fn proposal_for(slot: u64, payload_bytes: usize) -> Proposal {
    let command = command_for_size(payload_bytes);
    Proposal::new(
        ProposalPriority::MAX,
        CLIENT_ID,
        slot,
        AcceptedValue::from_command("bench", slot, 1, 1, LogHash::ZERO, &command),
    )
}

fn request_for(slot: u64, payload_bytes: usize) -> RecordRequest {
    let command = command_for_size(payload_bytes);
    RecordRequest {
        cluster_id: "bench".into(),
        epoch: 1,
        config_id: 1,
        config_digest: membership().digest(),
        slot,
        step: 4,
        proposal: proposal_for(slot, payload_bytes),
        command: Some(command),
    }
}

fn summary_for(slot: u64, config_digest: LogHash, proposal: Proposal) -> RecordSummary {
    RecordSummary {
        recorder_id: RECORDER_ID.into(),
        slot,
        config_id: 1,
        config_digest,
        step: 4,
        first_current: Some(proposal),
        aggregate_prior: None,
        decided: None,
    }
}

fn proof_for(slot: u64) -> DecisionProof {
    DecisionProof::FastPath {
        cluster_id: "bench".into(),
        slot,
        epoch: 1,
        config_id: 1,
        config_digest: membership().digest(),
        proposal: proposal_for(slot, 0),
        summaries: Vec::new(),
    }
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

struct ServerHandle {
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<(), String>>,
}

impl ServerHandle {
    async fn shutdown(mut self) -> Result<(), String> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task
            .await
            .map_err(|error| format!("server task failed: {error}"))?
    }
}

struct RunningCandidate {
    candidate: Candidate,
    client: Arc<dyn RecorderRpc>,
    prewarm: Arc<PrewarmGates>,
    server: ServerHandle,
}

async fn start_candidate(candidate: Candidate) -> Result<RunningCandidate, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    let (shutdown, receiver) = oneshot::channel();
    let shutdown_future = async move {
        let _ = receiver.await;
    };
    let recorder = DeterministicRecorder::default();
    let prewarm = recorder.prewarm.clone();
    let peers = peers();

    let task = match candidate {
        Candidate::TcpPostcard => tokio::spawn(serve_recorder_tcp(
            listener,
            recorder,
            peers,
            RECOVERY_GENERATION,
            shutdown_future,
        )),
        Candidate::TcpRkyv => tokio::spawn(serve_recorder_rkyv_tcp(
            listener,
            recorder,
            peers,
            RECOVERY_GENERATION,
            shutdown_future,
        )),
    };
    let client = client_for(candidate, address)?;
    Ok(RunningCandidate {
        candidate,
        client,
        prewarm,
        server: ServerHandle {
            shutdown: Some(shutdown),
            task,
        },
    })
}

fn client_for(candidate: Candidate, address: SocketAddr) -> Result<Arc<dyn RecorderRpc>, String> {
    let client: Arc<dyn RecorderRpc> = match candidate {
        Candidate::TcpPostcard => Arc::new(TcpPostcardRecorderClient::new(
            address,
            RECORDER_ID,
            CLIENT_ID,
            CLIENT_TOKEN,
            RECOVERY_GENERATION,
        )?),
        Candidate::TcpRkyv => Arc::new(TcpRkyvRecorderClient::new(
            address,
            RECORDER_ID,
            CLIENT_ID,
            CLIENT_TOKEN,
            RECOVERY_GENERATION,
        )?),
    };
    Ok(client)
}

struct CallFailure {
    class: &'static str,
    message: String,
}

fn call(client: &dyn RecorderRpc, workload: Workload, sequence: usize) -> Result<(), CallFailure> {
    let slot = sequence as u64 + 1;
    let result: rhiza_quepaxa::Result<()> = match workload {
        Workload::Identity => client.recorder_id().and_then(|recorder_id| {
            (recorder_id == RECORDER_ID)
                .then_some(())
                .ok_or_else(|| Error::Decode("benchmark recorder identity mismatch".into()))
        }),
        Workload::StoreCommand(payload_bytes) => {
            let command = command_for_size(payload_bytes);
            client.store_command_for(
                "bench".into(),
                1,
                1,
                membership().digest(),
                command.hash(),
                command,
            )
        }
        Workload::FetchCommand(payload_bytes) => {
            let expected = command_for_size(payload_bytes);
            client
                .fetch_command_for("bench".into(), 1, 1, membership().digest(), expected.hash())
                .and_then(|command| {
                    (command == Some(expected))
                        .then_some(())
                        .ok_or_else(|| Error::Decode("benchmark fetched command mismatch".into()))
                })
        }
        Workload::Record | Workload::RecordPayload(0) => {
            let request = request_for(slot, 0);
            let expected = summary_for(
                request.slot,
                request.config_digest,
                request.proposal.clone(),
            );
            client.record(request).and_then(|summary| {
                (summary == expected)
                    .then_some(())
                    .ok_or_else(|| Error::Decode("benchmark record summary mismatch".into()))
            })
        }
        Workload::RecordPayload(payload_bytes) => {
            let request = request_for(slot, payload_bytes);
            let expected = summary_for(
                request.slot,
                request.config_digest,
                request.proposal.clone(),
            );
            client.record(request).and_then(|summary| {
                (summary == expected)
                    .then_some(())
                    .ok_or_else(|| Error::Decode("benchmark record summary mismatch".into()))
            })
        }
        Workload::InstallDecisionProof => {
            client.install_decision_proof(proof_for(slot), &membership())
        }
        Workload::InspectDecisionProof => {
            let expected = proof_for(slot);
            client.inspect_decision_proof(slot).and_then(|proof| {
                (proof == Some(expected))
                    .then_some(())
                    .ok_or_else(|| Error::Decode("benchmark decision proof mismatch".into()))
            })
        }
        Workload::InspectRecordSummary => {
            let expected = summary_for(slot, membership().digest(), proposal_for(slot, 0));
            client.inspect_record_summary(slot).and_then(|summary| {
                (summary == Some(expected))
                    .then_some(())
                    .ok_or_else(|| Error::Decode("benchmark record summary mismatch".into()))
            })
        }
        Workload::ObserveReadFence => {
            let request = ReadFenceRequest {
                cluster_id: "bench".into(),
                epoch: 1,
                config_id: 1,
                config_digest: membership().digest(),
                slot,
            };
            client
                .observe_read_fence(request.clone())
                .and_then(|observed| {
                    let expected = ReadFenceObservation {
                        recorder_id: RECORDER_ID.into(),
                        cluster_id: request.cluster_id,
                        epoch: request.epoch,
                        config_id: request.config_id,
                        config_digest: request.config_digest,
                        slot: request.slot,
                        max_head: None,
                        slot_state: ReadFenceSlotState::Empty,
                    };
                    (observed == expected)
                        .then_some(())
                        .ok_or_else(|| Error::Decode("benchmark read-fence mismatch".into()))
                })
        }
    };
    match result {
        Ok(()) => Ok(()),
        Err(error) => Err(CallFailure {
            class: error_class(&error),
            message: error.to_string(),
        }),
    }
}

fn error_class(error: &Error) -> &'static str {
    match error {
        Error::Io(message) if message.contains("bridge overloaded") => "bridge_overloaded",
        Error::Io(message) if message.contains("overload") => "server_overloaded",
        Error::Io(message) if message.contains("timed out") || message.contains("deadline") => {
            "timeout"
        }
        Error::Io(_) => "io",
        Error::Decode(_) => "decode",
        Error::Rejected(_) => "rejected",
        Error::Cancelled => "cancelled",
        _ => "other",
    }
}

#[derive(Default)]
struct CallResults {
    successful_latency_ns: Vec<u64>,
    error_classes: BTreeMap<String, usize>,
    error_messages: Vec<ErrorMessageCount>,
    unrecorded_error_message_occurrences: usize,
}

#[derive(Clone, Serialize)]
struct ErrorMessageCount {
    message: String,
    count: usize,
}

impl CallResults {
    fn record(&mut self, started: Instant, result: Result<(), CallFailure>) {
        match result {
            Ok(()) => self
                .successful_latency_ns
                .push(started.elapsed().as_nanos().min(u64::MAX as u128) as u64),
            Err(failure) => {
                *self.error_classes.entry(failure.class.into()).or_default() += 1;
                self.record_error_message(failure.message, 1);
            }
        }
    }

    fn record_error_message(&mut self, message: String, count: usize) {
        if let Some(entry) = self
            .error_messages
            .iter_mut()
            .find(|entry| entry.message == message)
        {
            entry.count += count;
        } else if self.error_messages.len() < MAX_DISTINCT_ERROR_MESSAGES {
            self.error_messages
                .push(ErrorMessageCount { message, count });
        } else {
            self.unrecorded_error_message_occurrences += count;
        }
    }

    fn merge(&mut self, other: Self) {
        self.successful_latency_ns
            .extend(other.successful_latency_ns);
        for (class, count) in other.error_classes {
            *self.error_classes.entry(class).or_default() += count;
        }
        for entry in other.error_messages {
            self.record_error_message(entry.message, entry.count);
        }
        self.unrecorded_error_message_occurrences += other.unrecorded_error_message_occurrences;
    }

    fn errors(&self) -> usize {
        self.error_classes.values().sum()
    }
}

fn sequential_calls(
    client: &dyn RecorderRpc,
    workload: Workload,
    operations: usize,
) -> CallResults {
    let mut result = CallResults::default();
    for sequence in 0..operations {
        let started = Instant::now();
        result.record(started, call(client, workload, sequence));
    }
    result
}

fn measured_calls(
    client: Arc<dyn RecorderRpc>,
    workload: Workload,
    minimum_attempts_per_metric: usize,
    minimum_wall: Duration,
    concurrency: usize,
) -> (CallResults, Duration) {
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let completed = Arc::new(AtomicUsize::new(0));
    let (sender, receiver) = mpsc::channel();
    let mut threads = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let client = client.clone();
        let barrier = barrier.clone();
        let completed = completed.clone();
        let sender = sender.clone();
        threads.push(thread::spawn(move || {
            let mut local = CallResults::default();
            barrier.wait();
            let started_wall = Instant::now();
            let mut sequence = worker;
            loop {
                let started = Instant::now();
                local.record(started, call(client.as_ref(), workload, sequence));
                completed.fetch_add(1, Ordering::Relaxed);
                sequence += concurrency;
                if completed.load(Ordering::Relaxed) >= minimum_attempts_per_metric
                    && started_wall.elapsed() >= minimum_wall
                {
                    break;
                }
            }
            sender.send(local).unwrap();
        }));
    }
    drop(sender);
    let started = Instant::now();
    barrier.wait();
    let mut result = CallResults::default();
    for local in receiver {
        result.merge(local);
    }
    for worker in threads {
        worker.join().expect("benchmark worker panicked");
    }
    (result, started.elapsed())
}

fn concurrent_exact_calls(
    client: Arc<dyn RecorderRpc>,
    workload: Workload,
    operations: usize,
) -> CallResults {
    let barrier = Arc::new(Barrier::new(operations + 1));
    let mut threads = Vec::with_capacity(operations);
    for sequence in 0..operations {
        let client = client.clone();
        let barrier = barrier.clone();
        threads.push(thread::spawn(move || {
            barrier.wait();
            let mut result = CallResults::default();
            let started = Instant::now();
            result.record(started, call(client.as_ref(), workload, sequence));
            result
        }));
    }
    barrier.wait();
    let mut result = CallResults::default();
    for worker in threads {
        result.merge(worker.join().expect("prewarm worker panicked"));
    }
    result
}

fn prewarm_all_connections(
    client: Arc<dyn RecorderRpc>,
    gates: &PrewarmGates,
) -> (CallResults, usize) {
    gates.arm();
    let consensus = client.clone();
    let control = client;
    let consensus = thread::spawn(move || concurrent_exact_calls(consensus, Workload::Record, 2));
    let control =
        thread::spawn(move || concurrent_exact_calls(control, Workload::InspectRecordSummary, 2));
    let mut results = consensus.join().expect("consensus prewarm panicked");
    results.merge(control.join().expect("control prewarm panicked"));
    (results, gates.take_failures())
}

#[derive(Serialize)]
struct Metric {
    candidate: &'static str,
    cell_id: &'static str,
    workload: &'static str,
    payload_bytes: Option<usize>,
    lane: &'static str,
    security: &'static str,
    transport: &'static str,
    codec: &'static str,
    topology: &'static str,
    concurrency: usize,
    lane_prewarm_attempts: usize,
    lane_prewarm_errors: usize,
    lane_prewarm_gate_errors: usize,
    warmup_attempts: usize,
    warmup_errors: usize,
    attempts: usize,
    successes: usize,
    errors: usize,
    error_classes: BTreeMap<String, usize>,
    error_messages: Vec<ErrorMessageCount>,
    unrecorded_error_message_occurrences: usize,
    wall_seconds: f64,
    minimum_attempts_per_metric: usize,
    minimum_wall_seconds: f64,
    attempt_throughput_per_second: f64,
    success_throughput_per_second: f64,
    successful_latency_p50_us: Option<f64>,
    successful_latency_p95_us: Option<f64>,
    successful_latency_p99_us: Option<f64>,
    successful_latency_p999_us: Option<f64>,
    successful_latency_max_us: Option<f64>,
    length_prefix_bytes: usize,
    production_wire_version: u16,
    connections_per_lane: usize,
    diagnostic_valid: bool,
}

impl Metric {
    #[allow(clippy::too_many_arguments)]
    fn new(
        candidate: Candidate,
        workload: Workload,
        concurrency: usize,
        lane_prewarm: &CallResults,
        lane_prewarm_gate_errors: usize,
        warmup: &CallResults,
        mut measured: CallResults,
        wall: Duration,
        attempts: usize,
        minimum_attempts: usize,
        minimum_wall: Duration,
    ) -> Self {
        measured.successful_latency_ns.sort_unstable();
        let successes = measured.successful_latency_ns.len();
        let errors = measured.errors();
        let wall_seconds = wall.as_secs_f64();
        Self {
            candidate: candidate.name(),
            cell_id: workload.cell_id(),
            workload: workload.name(),
            payload_bytes: workload.payload_bytes(),
            lane: workload.lane(),
            security: "plaintext",
            transport: "production length-prefixed TCP recorder adapter",
            codec: candidate.codec(),
            topology: candidate.topology(),
            concurrency,
            lane_prewarm_attempts: lane_prewarm.successful_latency_ns.len() + lane_prewarm.errors(),
            lane_prewarm_errors: lane_prewarm.errors(),
            lane_prewarm_gate_errors,
            warmup_attempts: warmup.successful_latency_ns.len() + warmup.errors(),
            warmup_errors: warmup.errors(),
            attempts,
            successes,
            errors,
            error_classes: measured.error_classes,
            error_messages: measured.error_messages,
            unrecorded_error_message_occurrences: measured.unrecorded_error_message_occurrences,
            wall_seconds,
            minimum_attempts_per_metric: minimum_attempts,
            minimum_wall_seconds: minimum_wall.as_secs_f64(),
            attempt_throughput_per_second: attempts as f64 / wall_seconds,
            success_throughput_per_second: successes as f64 / wall_seconds,
            successful_latency_p50_us: percentile_us(&measured.successful_latency_ns, 0.5),
            successful_latency_p95_us: percentile_us(&measured.successful_latency_ns, 0.95),
            successful_latency_p99_us: percentile_us(&measured.successful_latency_ns, 0.99),
            successful_latency_p999_us: percentile_us(&measured.successful_latency_ns, 0.999),
            successful_latency_max_us: measured
                .successful_latency_ns
                .last()
                .map(|value| *value as f64 / 1_000.0),
            length_prefix_bytes: LENGTH_PREFIX_BYTES,
            production_wire_version: candidate.wire_version(),
            connections_per_lane: 2,
            diagnostic_valid: lane_prewarm.errors() == 0
                && lane_prewarm_gate_errors == 0
                && warmup.errors() == 0
                && errors == 0
                && successes + errors == attempts
                && attempts >= minimum_attempts
                && wall >= minimum_wall,
        }
    }
}

fn percentile_us(sorted_samples_ns: &[u64], quantile: f64) -> Option<f64> {
    if sorted_samples_ns.is_empty() {
        return None;
    }
    let rank = ((sorted_samples_ns.len() as f64 * quantile).ceil() as usize).max(1) - 1;
    Some(sorted_samples_ns[rank.min(sorted_samples_ns.len() - 1)] as f64 / 1_000.0)
}

#[derive(Serialize)]
struct Environment {
    git_commit: Option<String>,
    git_dirty: Option<bool>,
    rustc: Option<String>,
    os: Option<String>,
    cpu: Option<String>,
}

#[derive(Serialize)]
struct Conditions {
    host: &'static str,
    scope: &'static str,
    implementation: &'static str,
    fixture: &'static str,
    warmup_operations_per_metric: usize,
    minimum_attempts_per_metric: usize,
    minimum_duration_ms_per_metric: u64,
    connections_prewarmed_per_lane: usize,
    concurrency: Vec<usize>,
    candidates: Vec<&'static str>,
    candidate_order_offset: usize,
    cells: Vec<&'static str>,
    lane_warmup: &'static str,
    client_reuse: &'static str,
    measured_errors: &'static str,
    topology_invariant: &'static str,
    recorder_server_operation_cap: usize,
    excludes: &'static str,
}

#[derive(Serialize)]
struct Report {
    schema_version: u8,
    generated_at_epoch_seconds: f64,
    diagnostic_valid: bool,
    comparison_valid: bool,
    production_valid: bool,
    comparison_blockers: Vec<String>,
    environment: Environment,
    conditions: Conditions,
    metrics: Vec<Metric>,
}

fn environment() -> Environment {
    Environment {
        git_commit: command_output("git", &["rev-parse", "HEAD"]),
        git_dirty: git_dirty(),
        rustc: command_output("rustc", &["--version"]),
        os: command_output("uname", &["-a"]),
        cpu: command_output("sysctl", &["-n", "machdep.cpu.brand_string"])
            .or_else(|| command_output("sh", &["-c", "grep -m1 'model name' /proc/cpuinfo"])),
    }
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?.trim().to_owned();
    (!value.is_empty()).then_some(value)
}

fn git_dirty() -> Option<bool> {
    let output = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()?;
    output.status.success().then_some(!output.stdout.is_empty())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if env::var_os("RHIZA_BENCH_TRACING").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .try_init();
    }
    let config = Config::parse_from(&env::args().collect::<Vec<_>>()).unwrap_or_else(|error| {
        eprintln!("configuration error: {error}");
        print_usage();
        process::exit(2);
    });
    let mut candidates = Vec::new();
    for candidate in config.candidates.iter().copied() {
        candidates.push(start_candidate(candidate).await.unwrap_or_else(|error| {
            eprintln!("{} startup failed: {error}", candidate.name());
            process::exit(1);
        }));
    }

    let mut metrics = Vec::new();
    for running in &candidates {
        for workload in config.workloads.iter().copied() {
            for concurrency in config.concurrencies.iter().copied() {
                let client = running.client.clone();
                let warmup = config.warmup;
                let operations = config.operations;
                let minimum_wall = config.min_duration;
                let prewarm = running.prewarm.clone();
                let (lane_prewarm, lane_prewarm_gate_errors, warmed, measured, wall) =
                    tokio::task::spawn_blocking(move || {
                        let (lane_prewarm, lane_prewarm_gate_errors) =
                            prewarm_all_connections(client.clone(), &prewarm);
                        let warmed = sequential_calls(client.as_ref(), workload, warmup);
                        let (measured, wall) =
                            measured_calls(client, workload, operations, minimum_wall, concurrency);
                        (
                            lane_prewarm,
                            lane_prewarm_gate_errors,
                            warmed,
                            measured,
                            wall,
                        )
                    })
                    .await
                    .expect("benchmark phase task panicked");
                let attempts = measured.successful_latency_ns.len() + measured.errors();
                metrics.push(Metric::new(
                    running.candidate,
                    workload,
                    concurrency,
                    &lane_prewarm,
                    lane_prewarm_gate_errors,
                    &warmed,
                    measured,
                    wall,
                    attempts,
                    config.operations,
                    config.min_duration,
                ));
            }
        }
    }

    for running in candidates {
        running.server.shutdown().await.unwrap_or_else(|error| {
            eprintln!("{} shutdown failed: {error}", running.candidate.name());
            process::exit(1);
        });
    }

    let environment = environment();
    let diagnostic_valid = metrics.iter().all(|metric| metric.diagnostic_valid);
    let mut blockers = vec!["single raw run; use the balanced runner for comparison".into()];
    if environment.git_dirty != Some(false) {
        blockers.push("Git tree is dirty or its state is unknown".into());
    }
    if config.candidates.len() != 1 {
        blockers.push("raw run must contain exactly one exclusive transport candidate".into());
    }
    if !diagnostic_valid {
        blockers.push("one or more rows failed lane prewarm, warmup, or attempt accounting".into());
    }
    let report = Report {
        schema_version: 3,
        generated_at_epoch_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64(),
        diagnostic_valid,
        comparison_valid: false,
        production_valid: diagnostic_valid,
        comparison_blockers: blockers,
        environment,
        conditions: Conditions {
            host: "127.0.0.1 loopback; clients and servers in one process",
            scope: "one exclusive production RecorderRpc adapter per raw process; never negotiate, fall back, co-host candidates, or aggregate with rhiza-transport framework-only metrics",
            implementation: "public tcp-postcard and tcp-rkyv production server/client APIs with production HELLO, bounded framing, checked codec decoding, endpoint dispatch, deadlines, and identical connection-pool topology",
            fixture: "deterministic in-memory RecorderRpc with valid configured-client proposals and identical complete values for every candidate",
            warmup_operations_per_metric: config.warmup,
            minimum_attempts_per_metric: config.operations,
            minimum_duration_ms_per_metric: config.min_duration.as_millis() as u64,
            connections_prewarmed_per_lane: 2,
            concurrency: config.concurrencies,
            candidates: config.candidates.iter().map(|candidate| candidate.name()).collect(),
            candidate_order_offset: config.candidate_order_offset,
            cells: config.workloads.iter().map(|workload| workload.cell_id()).collect(),
            lane_warmup: "four concurrent calls open both pooled connections in both lanes before every metric warmup: two record calls and two inspect_record_summary calls",
            client_reuse: "exactly one shared production client object per peer/candidate; reused across all cells and threads",
            measured_errors: "all attempts count toward attempt throughput; errors remain classified with the first eight distinct messages/counts retained per cell; latency percentiles include successful calls only",
            topology_invariant: "both candidates use one shared client, distinct consensus/control pools, exactly two persistent connections per lane, and the same four-byte big-endian bounded frame prefix",
            recorder_server_operation_cap: RECORDER_SERVER_OPERATION_CAP,
            excludes: "QuePaxa quorum, persistence, fsync, materialization, remote network, resource profiling, certificate generation, and synthetic framework-only benchmark results",
        },
        metrics,
    };
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benchmark_matrix_covers_all_eight_operations_and_payload_cells() {
        assert_eq!(Workload::ALL.len(), 14);
        assert_eq!(
            Workload::ALL
                .iter()
                .map(|workload| workload.name())
                .collect::<std::collections::BTreeSet<_>>(),
            [
                "fetch_command",
                "identity",
                "inspect_decision_proof",
                "inspect_record_summary",
                "install_decision_proof",
                "observe_read_fence",
                "record",
                "store_command",
            ]
            .into_iter()
            .collect()
        );
        let cells = Workload::ALL
            .iter()
            .map(|workload| workload.cell_id())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(cells.len(), Workload::ALL.len());
        for operation in ["store_command", "fetch_command", "record"] {
            for payload in PAYLOAD_SIZES {
                assert!(cells.contains(format!("{operation}/payload-{payload}").as_str()));
            }
        }
    }

    #[test]
    fn cell_filter_accepts_only_unique_canonical_cell_ids() {
        assert_eq!(
            parse_cells("identity,record/payload-4096").unwrap(),
            vec![Workload::Identity, Workload::RecordPayload(4096)]
        );
        assert!(parse_cells("record/payload-1").is_err());
        assert!(parse_cells("identity,identity").is_err());
        assert!(parse_cells("").is_err());
    }

    #[test]
    fn measured_calls_reach_minimum_attempts_and_wall_time() {
        let recorder: Arc<dyn RecorderRpc> = Arc::new(DeterministicRecorder::default());
        let minimum_wall = Duration::from_millis(5);
        let (measured, wall) = measured_calls(recorder, Workload::Identity, 4, minimum_wall, 2);
        assert!(measured.successful_latency_ns.len() + measured.errors() >= 4);
        assert!(wall >= minimum_wall);
    }

    #[test]
    fn lane_prewarm_issues_exactly_two_concurrent_calls_per_lane() {
        let recorder = DeterministicRecorder::default();
        let gates = recorder.prewarm.clone();
        let recorder: Arc<dyn RecorderRpc> = Arc::new(recorder);
        let (prewarm, gate_errors) = prewarm_all_connections(recorder, &gates);
        assert_eq!(prewarm.successful_latency_ns.len() + prewarm.errors(), 4);
        assert_eq!(prewarm.errors(), 0);
        assert_eq!(gate_errors, 0);
    }

    #[test]
    fn prewarm_gate_holds_first_backend_call_until_second_arrives() {
        assert!(PREWARM_GATE_TIMEOUT < Duration::from_secs(10));
        let gate = Arc::new(PrewarmGate::default());
        gate.arm();
        let first_gate = gate.clone();
        let (finished, receiver) = mpsc::channel();
        let first = thread::spawn(move || {
            first_gate.wait();
            finished.send(()).unwrap();
        });
        assert!(receiver.recv_timeout(Duration::from_millis(10)).is_err());
        let second_gate = gate.clone();
        let second = thread::spawn(move || second_gate.wait());
        receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        first.join().unwrap();
        second.join().unwrap();
        assert_eq!(gate.take_failures(), 0);
    }

    #[test]
    fn prewarm_gate_timeout_releases_waiter_before_failure_is_collected() {
        let gate = Arc::new(PrewarmGate::default());
        gate.arm();
        let waiter_gate = gate.clone();
        let waiter = thread::spawn(move || waiter_gate.wait());
        waiter.join().unwrap();
        assert_eq!(gate.take_failures(), 1);
        assert_eq!(gate.take_failures(), 0);
    }

    #[test]
    fn metric_reports_truthful_codec_wire_and_identical_topology() {
        let prewarm = CallResults {
            successful_latency_ns: vec![1, 1, 1, 1],
            ..CallResults::default()
        };
        let warmup = CallResults {
            successful_latency_ns: vec![1],
            ..CallResults::default()
        };
        let measured = CallResults {
            successful_latency_ns: vec![1_000],
            ..CallResults::default()
        };
        let postcard = Metric::new(
            Candidate::TcpPostcard,
            Workload::RecordPayload(128),
            4,
            &prewarm,
            0,
            &warmup,
            measured,
            Duration::from_secs(1),
            1,
            1,
            Duration::from_secs(1),
        );
        let rkyv = Metric::new(
            Candidate::TcpRkyv,
            Workload::RecordPayload(128),
            4,
            &prewarm,
            0,
            &warmup,
            CallResults {
                successful_latency_ns: vec![1_000],
                ..CallResults::default()
            },
            Duration::from_secs(1),
            1,
            1,
            Duration::from_secs(1),
        );
        assert_eq!(postcard.codec, "postcard");
        assert_eq!(postcard.production_wire_version, POSTCARD_WIRE_VERSION);
        assert_eq!(rkyv.codec, "rkyv");
        assert_eq!(rkyv.production_wire_version, RKYV_WIRE_VERSION);
        assert_eq!(postcard.topology, rkyv.topology);
        assert_eq!(postcard.connections_per_lane, 2);
        assert_eq!(rkyv.connections_per_lane, 2);
        assert_eq!(postcard.cell_id, "record/payload-128");
        assert_eq!(postcard.payload_bytes, Some(128));
        assert_eq!(postcard.attempts, 1);
        assert_eq!(postcard.successes, 1);
        assert!(postcard.diagnostic_valid);
        assert!(rkyv.diagnostic_valid);
    }

    #[test]
    fn error_message_capture_bounds_distinct_values_and_counts_omitted_occurrences() {
        let mut results = CallResults::default();
        for index in 0..MAX_DISTINCT_ERROR_MESSAGES + 2 {
            results.record(
                Instant::now(),
                Err(CallFailure {
                    class: "io",
                    message: format!("wire failure {index}"),
                }),
            );
        }
        results.record(
            Instant::now(),
            Err(CallFailure {
                class: "io",
                message: "wire failure 0".into(),
            }),
        );

        assert_eq!(results.error_messages.len(), MAX_DISTINCT_ERROR_MESSAGES);
        assert_eq!(results.error_messages[0].count, 2);
        assert_eq!(results.unrecorded_error_message_occurrences, 2);
        assert_eq!(results.errors(), MAX_DISTINCT_ERROR_MESSAGES + 3);
    }

    #[test]
    fn config_rejects_mixed_transport_processes() {
        let args = [
            "bench".into(),
            "--candidates".into(),
            "tcp-postcard,tcp-rkyv".into(),
        ];
        assert!(Config::parse_from(&args)
            .err()
            .unwrap()
            .contains("exactly one exclusive transport"));
    }

    #[test]
    fn record_fixture_uses_an_admitted_client_proposal_with_a_command() {
        let request = request_for(7, 128);
        assert_eq!(request.proposal.proposer_id, CLIENT_ID);
        assert!(request.proposal.value.is_some());
        assert_eq!(request.command.as_ref().unwrap().payload.len(), 128);
        assert_eq!(request.config_digest, membership().digest());
    }
}
