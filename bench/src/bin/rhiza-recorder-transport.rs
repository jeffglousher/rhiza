use std::{
    collections::BTreeMap,
    env,
    net::SocketAddr,
    process::{self, Command},
    sync::{mpsc, Arc, Barrier},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rhiza_core::LogHash;
use rhiza_node::{
    serve_recorder_postcard_rpc, serve_recorder_postcard_rpc_tls, serve_recorder_tcp,
    serve_recorder_tcp_tls, PeerConfig, RecorderIngressLifecycle,
    RecorderPostcardRpcTlsClientConfig, RecorderPostcardRpcTlsServerConfig,
    RecorderTlsClientConfig, RecorderTlsServerConfig, TcpPostcardRecorderClient,
    TcpPostcardRpcRecorderClient,
};
use rhiza_quepaxa::{Error, Proposal, RecordRequest, RecordSummary, RecorderRpc};
use serde::Serialize;

const RECORDER_ID: &str = "node-1";
const CLIENT_ID: &str = "node-2";
const CLIENT_TOKEN: &str = "peer-token-2";
const RECOVERY_GENERATION: u64 = 7;
const POSTCARD_RPC_HEADER_BYTES: usize = 13;
const LENGTH_PREFIX_BYTES: usize = 4;
const MAX_DISTINCT_ERROR_MESSAGES: usize = 8;
const POSTCARD_RPC_LANE_IN_FLIGHT: usize = 8;
const POSTCARD_RPC_BRIDGE_DEPTH: usize = 128;
const RECORDER_SERVER_OPERATION_CAP: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[allow(clippy::enum_variant_names)]
enum Candidate {
    TcpPostcard,
    TcpPostcardRpc,
    TcpTlsPostcard,
    TcpTlsPostcardRpc,
}

impl Candidate {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "tcp-postcard" => Some(Self::TcpPostcard),
            "tcp-postcard-rpc" => Some(Self::TcpPostcardRpc),
            "tcp-tls-postcard" => Some(Self::TcpTlsPostcard),
            "tcp-tls-postcard-rpc" => Some(Self::TcpTlsPostcardRpc),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::TcpPostcard => "tcp-postcard",
            Self::TcpPostcardRpc => "tcp-postcard-rpc",
            Self::TcpTlsPostcard => "tcp-tls-postcard",
            Self::TcpTlsPostcardRpc => "tcp-tls-postcard-rpc",
        }
    }

    fn is_rpc(self) -> bool {
        matches!(self, Self::TcpPostcardRpc | Self::TcpTlsPostcardRpc)
    }

    fn is_tls(self) -> bool {
        matches!(self, Self::TcpTlsPostcard | Self::TcpTlsPostcardRpc)
    }

    fn topology(self) -> &'static str {
        if self.is_rpc() {
            "one shared production client; one persistent postcard-rpc session per consensus/control lane; lane in-flight=8; bridge depth=128; server operation cap=32; try_send overload is reported"
        } else {
            "one shared production client; two pooled production connections per consensus/control lane; callers queue while both are busy"
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Workload {
    Record,
    InspectRecordSummary,
}

impl Workload {
    const ALL: [Self; 2] = [Self::Record, Self::InspectRecordSummary];

    fn name(self) -> &'static str {
        match self {
            Self::Record => "record",
            Self::InspectRecordSummary => "inspect_record_summary",
        }
    }

    fn lane(self) -> &'static str {
        match self {
            Self::Record => "consensus",
            Self::InspectRecordSummary => "control",
        }
    }
}

struct Config {
    warmup: usize,
    operations: usize,
    concurrencies: Vec<usize>,
    candidates: Vec<Candidate>,
    candidate_order_offset: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            warmup: 1_000,
            operations: 10_000,
            concurrencies: vec![1, 4, 32],
            candidates: vec![Candidate::TcpPostcard, Candidate::TcpPostcardRpc],
            candidate_order_offset: 0,
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
                "--concurrency" => config.concurrencies = parse_positive_list(value, flag)?,
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

fn print_usage() {
    eprintln!(
        "Usage: rhiza-recorder-transport [--warmup N] [--operations N] \
         [--concurrency N,N] [--candidates NAME,NAME] [--candidate-order-offset N]\n\
         Candidates: tcp-postcard,tcp-postcard-rpc,tcp-tls-postcard,tcp-tls-postcard-rpc"
    );
}

#[derive(Clone, Default)]
struct DeterministicRecorder;

impl RecorderRpc for DeterministicRecorder {
    fn recorder_id(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        Ok(RECORDER_ID.into())
    }

    fn record(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        request: RecordRequest,
    ) -> rhiza_quepaxa::Result<RecordSummary> {
        Ok(summary_for(request.slot))
    }

    fn inspect_record_summary(
        &self,
        _context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        Ok(Some(summary_for(slot)))
    }
}

fn request_for(slot: u64) -> RecordRequest {
    RecordRequest {
        cluster_id: "bench".into(),
        epoch: 1,
        config_id: 1,
        config_digest: LogHash::ZERO,
        slot,
        step: 2,
        proposal: Proposal::nil(),
        command: None,
    }
}

fn summary_for(slot: u64) -> RecordSummary {
    RecordSummary {
        recorder_id: RECORDER_ID.into(),
        slot,
        config_id: 1,
        config_digest: LogHash::ZERO,
        step: 2,
        first_current: None,
        aggregate_prior: None,
        decided: None,
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

struct TlsMaterial {
    cert_pem: String,
    key_pem: String,
}

fn tls_material() -> TlsMaterial {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["recorder.test".into()]).unwrap();
    TlsMaterial {
        cert_pem: cert.pem(),
        key_pem: signing_key.serialize_pem(),
    }
}

struct ServerHandle {
    shutdown: Option<tokio::sync::watch::Sender<bool>>,
    _force: tokio::sync::watch::Sender<bool>,
    _started: tokio::sync::oneshot::Receiver<()>,
    _listener_dropped: tokio::sync::oneshot::Receiver<()>,
    task: tokio::task::JoinHandle<Result<(), String>>,
}

impl ServerHandle {
    async fn shutdown(mut self) -> Result<(), String> {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send_replace(true);
        }
        self.task
            .await
            .map_err(|error| format!("server task failed: {error}"))?
    }
}

struct RunningCandidate {
    candidate: Candidate,
    client: Arc<dyn RecorderRpc>,
    server: ServerHandle,
}

async fn start_candidate(
    candidate: Candidate,
    tls: &TlsMaterial,
) -> Result<RunningCandidate, String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let address = listener.local_addr().map_err(|error| error.to_string())?;
    let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let (force, force_rx) = tokio::sync::watch::channel(false);
    let (started, started_rx) = tokio::sync::oneshot::channel();
    let (listener_dropped, listener_dropped_rx) = tokio::sync::oneshot::channel();
    let lifecycle = RecorderIngressLifecycle::new(shutdown_rx, force_rx, started, listener_dropped);
    let recorder = DeterministicRecorder;
    let peers = peers();

    let task = match candidate {
        Candidate::TcpPostcard => tokio::spawn(async move {
            serve_recorder_tcp(listener, recorder, peers, RECOVERY_GENERATION, lifecycle)
                .await
                .result
        }),
        Candidate::TcpPostcardRpc => tokio::spawn(async move {
            serve_recorder_postcard_rpc(listener, recorder, peers, RECOVERY_GENERATION, lifecycle)
                .await
                .result
        }),
        Candidate::TcpTlsPostcard => {
            let config =
                RecorderTlsServerConfig::from_pem(tls.cert_pem.as_bytes(), tls.key_pem.as_bytes())?;
            tokio::spawn(async move {
                serve_recorder_tcp_tls(
                    listener,
                    recorder,
                    peers,
                    RECOVERY_GENERATION,
                    config,
                    lifecycle,
                )
                .await
                .result
            })
        }
        Candidate::TcpTlsPostcardRpc => {
            let config = RecorderPostcardRpcTlsServerConfig::from_pem(
                tls.cert_pem.as_bytes(),
                tls.key_pem.as_bytes(),
            )?;
            tokio::spawn(async move {
                serve_recorder_postcard_rpc_tls(
                    listener,
                    recorder,
                    peers,
                    RECOVERY_GENERATION,
                    config,
                    lifecycle,
                )
                .await
                .result
            })
        }
    };
    let client = client_for(candidate, address, tls)?;
    Ok(RunningCandidate {
        candidate,
        client,
        server: ServerHandle {
            shutdown: Some(shutdown),
            _force: force,
            _started: started_rx,
            _listener_dropped: listener_dropped_rx,
            task,
        },
    })
}

fn client_for(
    candidate: Candidate,
    address: SocketAddr,
    tls: &TlsMaterial,
) -> Result<Arc<dyn RecorderRpc>, String> {
    let client: Arc<dyn RecorderRpc> = match candidate {
        Candidate::TcpPostcard => Arc::new(TcpPostcardRecorderClient::new(
            address,
            RECORDER_ID,
            CLIENT_ID,
            CLIENT_TOKEN,
            RECOVERY_GENERATION,
        )?),
        Candidate::TcpPostcardRpc => Arc::new(TcpPostcardRpcRecorderClient::new(
            address,
            RECORDER_ID,
            CLIENT_ID,
            CLIENT_TOKEN,
            RECOVERY_GENERATION,
        )?),
        Candidate::TcpTlsPostcard => {
            let config =
                RecorderTlsClientConfig::from_ca_pem(tls.cert_pem.as_bytes(), "recorder.test")?;
            Arc::new(TcpPostcardRecorderClient::new_tls(
                address,
                RECORDER_ID,
                CLIENT_ID,
                CLIENT_TOKEN,
                RECOVERY_GENERATION,
                config,
            )?)
        }
        Candidate::TcpTlsPostcardRpc => {
            let config = RecorderPostcardRpcTlsClientConfig::from_ca_pem(
                tls.cert_pem.as_bytes(),
                "recorder.test",
            )?;
            Arc::new(TcpPostcardRpcRecorderClient::new_tls(
                address,
                RECORDER_ID,
                CLIENT_ID,
                CLIENT_TOKEN,
                RECOVERY_GENERATION,
                config,
            )?)
        }
    };
    Ok(client)
}

struct CallFailure {
    class: &'static str,
    message: String,
}

fn call(client: &dyn RecorderRpc, workload: Workload, sequence: usize) -> Result<(), CallFailure> {
    let slot = sequence as u64 + 1;
    let context = rhiza_quepaxa::RecorderRpcContext::default_timeout();
    let result = match workload {
        Workload::Record => client.record(&context, request_for(slot)),
        Workload::InspectRecordSummary => client
            .inspect_record_summary(&context, slot)
            .and_then(|summary| summary.ok_or(Error::TypedRecordRequired)),
    };
    match result {
        Ok(summary) if summary == summary_for(slot) => Ok(()),
        Ok(_) => Err(CallFailure {
            class: "semantic_mismatch",
            message: "response did not match the deterministic RecorderRpc fixture".into(),
        }),
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
    operations: usize,
    concurrency: usize,
) -> (CallResults, Duration) {
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let (sender, receiver) = mpsc::channel();
    let mut threads = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let client = client.clone();
        let barrier = barrier.clone();
        let sender = sender.clone();
        threads.push(thread::spawn(move || {
            let mut local = CallResults::default();
            barrier.wait();
            for sequence in (worker..operations).step_by(concurrency) {
                let started = Instant::now();
                local.record(started, call(client.as_ref(), workload, sequence));
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

#[derive(Serialize)]
struct Metric {
    candidate: &'static str,
    workload: &'static str,
    lane: &'static str,
    security: &'static str,
    transport: &'static str,
    codec: &'static str,
    topology: &'static str,
    concurrency: usize,
    lane_prewarm_attempts: usize,
    lane_prewarm_errors: usize,
    warmup_attempts: usize,
    warmup_errors: usize,
    attempts: usize,
    successes: usize,
    errors: usize,
    error_classes: BTreeMap<String, usize>,
    error_messages: Vec<ErrorMessageCount>,
    unrecorded_error_message_occurrences: usize,
    wall_seconds: f64,
    attempt_throughput_per_second: f64,
    success_throughput_per_second: f64,
    successful_latency_p50_us: Option<f64>,
    successful_latency_p95_us: Option<f64>,
    successful_latency_p99_us: Option<f64>,
    successful_latency_p999_us: Option<f64>,
    successful_latency_max_us: Option<f64>,
    postcard_rpc_header_bytes: Option<usize>,
    length_prefix_bytes: usize,
    production_wire_version: u16,
    diagnostic_valid: bool,
}

impl Metric {
    #[allow(clippy::too_many_arguments)]
    fn new(
        candidate: Candidate,
        workload: Workload,
        concurrency: usize,
        lane_prewarm: &CallResults,
        warmup: &CallResults,
        mut measured: CallResults,
        wall: Duration,
        attempts: usize,
    ) -> Self {
        measured.successful_latency_ns.sort_unstable();
        let successes = measured.successful_latency_ns.len();
        let errors = measured.errors();
        let wall_seconds = wall.as_secs_f64();
        Self {
            candidate: candidate.name(),
            workload: workload.name(),
            lane: workload.lane(),
            security: if candidate.is_tls() {
                "TLS 1.3 server-authenticated"
            } else {
                "plaintext"
            },
            transport: "production length-prefixed TCP recorder adapter",
            codec: "production opaque postcard envelope",
            topology: candidate.topology(),
            concurrency,
            lane_prewarm_attempts: 2,
            lane_prewarm_errors: lane_prewarm.errors(),
            warmup_attempts: warmup.successful_latency_ns.len() + warmup.errors(),
            warmup_errors: warmup.errors(),
            attempts,
            successes,
            errors,
            error_classes: measured.error_classes,
            error_messages: measured.error_messages,
            unrecorded_error_message_occurrences: measured.unrecorded_error_message_occurrences,
            wall_seconds,
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
            postcard_rpc_header_bytes: candidate.is_rpc().then_some(POSTCARD_RPC_HEADER_BYTES),
            length_prefix_bytes: LENGTH_PREFIX_BYTES,
            production_wire_version: if candidate.is_rpc() { 2 } else { 1 },
            diagnostic_valid: lane_prewarm.errors() == 0
                && warmup.errors() == 0
                && successes + errors == attempts,
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
    measured_operations_per_metric: usize,
    concurrency: Vec<usize>,
    candidates: Vec<&'static str>,
    candidate_order_offset: usize,
    workloads: [&'static str; 2],
    lane_warmup: &'static str,
    client_reuse: &'static str,
    measured_errors: &'static str,
    postcard_rpc_context: &'static str,
    postcard_rpc_lane_in_flight: usize,
    postcard_rpc_bridge_depth: usize,
    recorder_server_operation_cap: usize,
    tls: &'static str,
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
    let tls = tls_material();
    let mut candidates = Vec::new();
    for candidate in config.candidates.iter().copied() {
        candidates.push(
            start_candidate(candidate, &tls)
                .await
                .unwrap_or_else(|error| {
                    eprintln!("{} startup failed: {error}", candidate.name());
                    process::exit(1);
                }),
        );
    }

    let mut metrics = Vec::new();
    for running in &candidates {
        for workload in Workload::ALL {
            for concurrency in config.concurrencies.iter().copied() {
                let client = running.client.clone();
                let warmup = config.warmup;
                let operations = config.operations;
                let (lane_prewarm, warmed, measured, wall) =
                    tokio::task::spawn_blocking(move || {
                        let mut lane_prewarm = CallResults::default();
                        for lane in Workload::ALL {
                            lane_prewarm.merge(sequential_calls(client.as_ref(), lane, 1));
                        }
                        let warmed = sequential_calls(client.as_ref(), workload, warmup);
                        let (measured, wall) =
                            measured_calls(client, workload, operations, concurrency);
                        (lane_prewarm, warmed, measured, wall)
                    })
                    .await
                    .expect("benchmark phase task panicked");
                metrics.push(Metric::new(
                    running.candidate,
                    workload,
                    concurrency,
                    &lane_prewarm,
                    &warmed,
                    measured,
                    wall,
                    config.operations,
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
    let includes_plaintext = config
        .candidates
        .iter()
        .any(|candidate| !candidate.is_tls());
    let includes_tls = config.candidates.iter().any(|candidate| candidate.is_tls());
    if includes_plaintext && includes_tls {
        blockers.push("candidate set mixes plaintext and TLS strata".into());
    }
    if !diagnostic_valid {
        blockers.push("one or more rows failed lane prewarm, warmup, or attempt accounting".into());
    }
    let report = Report {
        schema_version: 1,
        generated_at_epoch_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64(),
        diagnostic_valid,
        comparison_valid: false,
        production_valid: true,
        comparison_blockers: blockers,
        environment,
        conditions: Conditions {
            host: "127.0.0.1 loopback; clients and servers in one process",
            scope: "production RecorderRpc adapter A/B only; never aggregate with rhiza-transport framework-only metrics",
            implementation: "public legacy and postcard-rpc production server/client APIs with production HELLO, opaque postcard bodies, sync bridges, endpoint dispatch, deadlines, and connection/session topology",
            fixture: "stateless deterministic in-memory RecorderRpc with identical semantics for every candidate",
            warmup_operations_per_metric: config.warmup,
            measured_operations_per_metric: config.operations,
            concurrency: config.concurrencies,
            candidates: config.candidates.iter().map(|candidate| candidate.name()).collect(),
            candidate_order_offset: config.candidate_order_offset,
            workloads: ["record", "inspect_record_summary"],
            lane_warmup: "both consensus and control lanes are called sequentially before every metric warmup",
            client_reuse: "exactly one shared production client object per peer/candidate; reused across all cells and threads",
            measured_errors: "all attempts count toward attempt throughput; errors remain classified with the first eight distinct messages/counts retained per cell; latency percentiles include successful calls only",
            postcard_rpc_context: "production Key8 + Seq4 VarHeader is 13 bytes (1 discriminator + 8 key + 4 sequence); opaque endpoint body and 4-byte frame length prefix are additional",
            postcard_rpc_lane_in_flight: POSTCARD_RPC_LANE_IN_FLIGHT,
            postcard_rpc_bridge_depth: POSTCARD_RPC_BRIDGE_DEPTH,
            recorder_server_operation_cap: RECORDER_SERVER_OPERATION_CAP,
            tls: "optional TLS 1.3 stratum uses the same generated certificate/root and identical fixture semantics; production ALPNs remain distinct",
            excludes: "QuePaxa quorum, persistence, fsync, materialization, remote network, resource profiling, certificate generation, and synthetic framework-only benchmark results",
        },
        metrics,
    };
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}

#[cfg(test)]
mod tests {
    use postcard_rpc::{
        header::{VarHeader, VarKey, VarSeq},
        Key,
    };

    use super::*;

    #[test]
    fn production_postcard_rpc_context_uses_a_thirteen_byte_key8_seq4_header() {
        let header = VarHeader {
            key: VarKey::Key8(Key::for_path::<u8>("bench/header")),
            seq_no: VarSeq::Seq4(42),
        };
        assert_eq!(header.write_to_vec().len(), POSTCARD_RPC_HEADER_BYTES);
    }

    #[test]
    fn shared_client_overload_is_retained_in_attempt_and_success_reporting() {
        let error = Error::Io("recorder postcard-rpc bridge overloaded".into());
        assert_eq!(error_class(&error), "bridge_overloaded");
        let prewarm = CallResults {
            successful_latency_ns: vec![1, 1],
            ..CallResults::default()
        };
        let warmup = CallResults {
            successful_latency_ns: vec![1],
            ..CallResults::default()
        };
        let mut measured = CallResults {
            successful_latency_ns: vec![1_000],
            ..CallResults::default()
        };
        measured.record(
            Instant::now(),
            Err(CallFailure {
                class: error_class(&error),
                message: error.to_string(),
            }),
        );
        let metric = Metric::new(
            Candidate::TcpPostcardRpc,
            Workload::Record,
            32,
            &prewarm,
            &warmup,
            measured,
            Duration::from_secs(1),
            2,
        );
        assert_eq!(metric.attempts, 2);
        assert_eq!(metric.successes, 1);
        assert_eq!(metric.errors, 1);
        assert_eq!(metric.error_classes["bridge_overloaded"], 1);
        assert_eq!(metric.error_messages.len(), 1);
        assert_eq!(
            metric.error_messages[0].message,
            "QuePaxa io failed: recorder postcard-rpc bridge overloaded"
        );
        assert_eq!(metric.error_messages[0].count, 1);
        assert_eq!(metric.unrecorded_error_message_occurrences, 0);
        assert_eq!(metric.attempt_throughput_per_second, 2.0);
        assert_eq!(metric.success_throughput_per_second, 1.0);
        assert!(metric.topology.contains("one shared production client"));
        assert!(metric.diagnostic_valid);
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
    fn config_rotates_each_selected_pair_without_changing_membership() {
        let args = [
            "bench".into(),
            "--candidates".into(),
            "tcp-postcard,tcp-postcard-rpc".into(),
            "--candidate-order-offset".into(),
            "1".into(),
        ];
        let config = Config::parse_from(&args).unwrap();
        assert_eq!(
            config
                .candidates
                .iter()
                .map(|candidate| candidate.name())
                .collect::<Vec<_>>(),
            ["tcp-postcard-rpc", "tcp-postcard"]
        );
    }
}
