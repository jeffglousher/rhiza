use std::{
    collections::BTreeSet,
    env,
    hint::black_box,
    process::{self, Command},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rkyv::{rancor::Error as RkyvError, util::AlignedVec};
use serde::Serialize;

const PAYLOAD_BYTES: [usize; 3] = [0, 128, 4 * 1024];
const WIRE_VERSION: u16 = 3;
const CARGO_PROFILE: &str = env!("RHIZA_BENCH_CARGO_PROFILE");
const CARGO_OPT_LEVEL: &str = env!("RHIZA_BENCH_CARGO_OPT_LEVEL");

macro_rules! wire_derives {
    ($item:item) => {
        #[derive(
            Clone,
            Debug,
            Eq,
            PartialEq,
            serde::Deserialize,
            serde::Serialize,
            rkyv::Archive,
            rkyv::Deserialize,
            rkyv::Serialize,
        )]
        $item
    };
}

wire_derives! {
    struct LogHash([u8; 32]);
}

wire_derives! {
    enum EntryType {
        Command,
        ConfigChange,
        SnapshotBarrier,
        SnapshotPublished,
        Noop,
    }
}

wire_derives! {
    struct StoredCommand {
        entry_type: EntryType,
        payload: Vec<u8>,
    }
}

wire_derives! {
    struct ProposalPriority([u8; 32]);
}

wire_derives! {
    struct AcceptedValue {
        command_hash: LogHash,
        prev_hash: LogHash,
        entry_hash: LogHash,
    }
}

wire_derives! {
    struct Proposal {
        priority: ProposalPriority,
        proposer_id: String,
        proposal_id: u64,
        value: Option<AcceptedValue>,
    }
}

wire_derives! {
    struct RecorderSummary {
        recorder_id: String,
        slot: u64,
        step: u64,
        first_current: Option<Proposal>,
        aggregate_prior: Option<Proposal>,
    }
}

wire_derives! {
    enum DecisionProof {
        FastPath {
            cluster_id: String,
            slot: u64,
            epoch: u64,
            config_id: u64,
            config_digest: LogHash,
            proposal: Proposal,
            summaries: Vec<RecorderSummary>,
        },
        Phase2 {
            cluster_id: String,
            slot: u64,
            epoch: u64,
            config_id: u64,
            config_digest: LogHash,
            step: u64,
            proposal: Proposal,
            summaries: Vec<RecorderSummary>,
        },
    }
}

wire_derives! {
    struct RecordRequest {
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        slot: u64,
        step: u64,
        proposal: Proposal,
        #[serde(default)]
        command: Option<StoredCommand>,
    }
}

wire_derives! {
    struct RecordSummary {
        recorder_id: String,
        slot: u64,
        config_id: u64,
        config_digest: LogHash,
        step: u64,
        first_current: Option<Proposal>,
        aggregate_prior: Option<Proposal>,
        decided: Option<DecisionProof>,
    }
}

wire_derives! {
    struct ReadFenceRequest {
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        slot: u64,
    }
}

wire_derives! {
    enum ReadFenceSlotState {
        Empty,
        Occupied {
            summary: Option<Box<RecordSummary>>,
        },
    }
}

wire_derives! {
    struct ReadFenceObservation {
        recorder_id: String,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        slot: u64,
        max_head: Option<u64>,
        slot_state: ReadFenceSlotState,
    }
}

wire_derives! {
    struct Ballot {
        round: u64,
        priority: u128,
        proposer_id: String,
    }
}

wire_derives! {
    enum RejectReason {
        StaleEpoch,
        FutureEpoch,
        WrongCluster,
        WrongConfig,
        WrongSlot,
        AlreadyDecided,
        MalformedDecision,
        BallotPromised { promised: Ballot },
        ConflictingValue,
        InvalidValue,
        InvalidCertificate,
        ConfigurationSealed { stop_slot: u64 },
        ConfigurationNotInstalled,
        ActivationRequired,
        TransitionInProgress,
        InvalidTransition,
        LocalVoterRequired,
        StepRegression,
        InvalidRequest,
    }
}

wire_derives! {
    enum RpcResult<T> {
        Ok(T),
        Rejected(RejectReason),
        Error(String),
        Overloaded,
    }
}

wire_derives! {
    struct RequestFrame {
        version: u16,
        request_id: u64,
        remaining_deadline_ms: u32,
        body: RecorderRequestBody,
    }
}

wire_derives! {
    enum RecorderRequestBody {
        Identity,
        StoreCommand {
            cluster_id: String,
            epoch: u64,
            config_id: u64,
            config_digest: LogHash,
            command_hash: LogHash,
            command: StoredCommand,
        },
        FetchCommand {
            cluster_id: String,
            epoch: u64,
            config_id: u64,
            config_digest: LogHash,
            command_hash: LogHash,
        },
        Record(RecordRequest),
        InstallDecisionProof {
            proof: DecisionProof,
            members: Vec<String>,
        },
        InspectDecisionProof {
            slot: u64,
        },
        InspectRecordSummary {
            slot: u64,
        },
        ObserveReadFence(ReadFenceRequest),
    }
}

wire_derives! {
    struct ResponseFrame {
        version: u16,
        request_id: u64,
        body: RecorderResponseBody,
    }
}

wire_derives! {
    enum RecorderResponseBody {
        Identity(RpcResult<String>),
        StoreCommand(RpcResult<()>),
        FetchCommand(RpcResult<Option<StoredCommand>>),
        Record(RpcResult<RecordSummary>),
        InstallDecisionProof(RpcResult<()>),
        InspectDecisionProof(RpcResult<Option<DecisionProof>>),
        InspectRecordSummary(RpcResult<Option<RecordSummary>>),
        ObserveReadFence(RpcResult<ReadFenceObservation>),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
// Boxing either side would add benchmark-only allocation to a timed decode.
#[allow(clippy::large_enum_variant)]
enum WireValue {
    Request(RequestFrame),
    Response(ResponseFrame),
}

#[derive(Clone)]
struct SemanticCell {
    id: String,
    operation: &'static str,
    direction: &'static str,
    command_payload_bytes: Option<usize>,
    value: WireValue,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Codec {
    Postcard,
    Rkyv,
}

impl Codec {
    const ALL: [Self; 2] = [Self::Postcard, Self::Rkyv];

    fn name(self) -> &'static str {
        match self {
            Self::Postcard => "postcard",
            Self::Rkyv => "rkyv",
        }
    }

    fn version(self) -> &'static str {
        match self {
            Self::Postcard => "1.1.3 (bench/Cargo.lock)",
            Self::Rkyv => "0.8.17",
        }
    }
}

enum Encoded {
    Postcard(Vec<u8>),
    Rkyv(AlignedVec),
}

impl Encoded {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Postcard(bytes) => bytes,
            Self::Rkyv(bytes) => bytes,
        }
    }
}

fn encode(codec: Codec, value: &WireValue) -> Result<Encoded, String> {
    match (codec, value) {
        (Codec::Postcard, WireValue::Request(value)) => postcard::to_allocvec(value)
            .map(Encoded::Postcard)
            .map_err(|error| error.to_string()),
        (Codec::Postcard, WireValue::Response(value)) => postcard::to_allocvec(value)
            .map(Encoded::Postcard)
            .map_err(|error| error.to_string()),
        (Codec::Rkyv, WireValue::Request(value)) => rkyv::to_bytes::<RkyvError>(value)
            .map(Encoded::Rkyv)
            .map_err(|error| error.to_string()),
        (Codec::Rkyv, WireValue::Response(value)) => rkyv::to_bytes::<RkyvError>(value)
            .map(Encoded::Rkyv)
            .map_err(|error| error.to_string()),
    }
}

fn decode(codec: Codec, template: &WireValue, network_bytes: &[u8]) -> Result<WireValue, String> {
    match (codec, template) {
        (Codec::Postcard, WireValue::Request(_)) => postcard::from_bytes(network_bytes)
            .map(WireValue::Request)
            .map_err(|error| error.to_string()),
        (Codec::Postcard, WireValue::Response(_)) => postcard::from_bytes(network_bytes)
            .map(WireValue::Response)
            .map_err(|error| error.to_string()),
        (Codec::Rkyv, WireValue::Request(_)) => {
            rkyv_network_decode::<RequestFrame>(network_bytes).map(WireValue::Request)
        }
        (Codec::Rkyv, WireValue::Response(_)) => {
            rkyv_network_decode::<ResponseFrame>(network_bytes).map(WireValue::Response)
        }
    }
}

fn rkyv_network_decode<T>(network_bytes: &[u8]) -> Result<T, String>
where
    T: rkyv::Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, RkyvError>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<RkyvError>>,
{
    // Socket buffers do not promise root alignment. This copy, checked
    // validation, and complete owned materialization are all measured.
    let mut aligned: AlignedVec = AlignedVec::with_capacity(network_bytes.len());
    aligned.extend_from_slice(network_bytes);
    rkyv::from_bytes::<T, RkyvError>(&aligned).map_err(|error| error.to_string())
}

fn semantic_cells() -> Vec<SemanticCell> {
    let mut cells = Vec::with_capacity(22);
    cells.push(request_cell(
        "identity",
        None,
        RecorderRequestBody::Identity,
    ));
    cells.push(response_cell(
        "identity",
        None,
        RecorderResponseBody::Identity(RpcResult::Ok("node-1".into())),
    ));

    for payload_bytes in PAYLOAD_BYTES {
        let command = stored_command(payload_bytes);
        cells.push(request_cell(
            "store_command",
            Some(payload_bytes),
            RecorderRequestBody::StoreCommand {
                cluster_id: "bench".into(),
                epoch: 7,
                config_id: 11,
                config_digest: hash(11),
                command_hash: hash(payload_bytes),
                command,
            },
        ));
    }
    cells.push(response_cell(
        "store_command",
        None,
        RecorderResponseBody::StoreCommand(RpcResult::Ok(())),
    ));

    cells.push(request_cell(
        "fetch_command",
        None,
        RecorderRequestBody::FetchCommand {
            cluster_id: "bench".into(),
            epoch: 7,
            config_id: 11,
            config_digest: hash(11),
            command_hash: hash(42),
        },
    ));
    for payload_bytes in PAYLOAD_BYTES {
        cells.push(response_cell(
            "fetch_command",
            Some(payload_bytes),
            RecorderResponseBody::FetchCommand(RpcResult::Ok(Some(stored_command(payload_bytes)))),
        ));
    }

    for payload_bytes in PAYLOAD_BYTES {
        cells.push(request_cell(
            "record",
            Some(payload_bytes),
            RecorderRequestBody::Record(record_request(payload_bytes)),
        ));
    }
    cells.push(response_cell(
        "record",
        None,
        RecorderResponseBody::Record(RpcResult::Ok(record_summary())),
    ));

    cells.push(request_cell(
        "install_decision_proof",
        None,
        RecorderRequestBody::InstallDecisionProof {
            proof: decision_proof(),
            members: vec!["node-1".into(), "node-2".into(), "node-3".into()],
        },
    ));
    cells.push(response_cell(
        "install_decision_proof",
        None,
        RecorderResponseBody::InstallDecisionProof(RpcResult::Ok(())),
    ));
    cells.push(request_cell(
        "inspect_decision_proof",
        None,
        RecorderRequestBody::InspectDecisionProof { slot: 42 },
    ));
    cells.push(response_cell(
        "inspect_decision_proof",
        None,
        RecorderResponseBody::InspectDecisionProof(RpcResult::Ok(Some(decision_proof()))),
    ));
    cells.push(request_cell(
        "inspect_record_summary",
        None,
        RecorderRequestBody::InspectRecordSummary { slot: 42 },
    ));
    cells.push(response_cell(
        "inspect_record_summary",
        None,
        RecorderResponseBody::InspectRecordSummary(RpcResult::Ok(Some(record_summary()))),
    ));
    let fence = read_fence_request();
    cells.push(request_cell(
        "observe_read_fence",
        None,
        RecorderRequestBody::ObserveReadFence(fence.clone()),
    ));
    cells.push(response_cell(
        "observe_read_fence",
        None,
        RecorderResponseBody::ObserveReadFence(RpcResult::Ok(ReadFenceObservation {
            recorder_id: "node-1".into(),
            cluster_id: fence.cluster_id,
            epoch: fence.epoch,
            config_id: fence.config_id,
            config_digest: fence.config_digest,
            slot: fence.slot,
            max_head: Some(42),
            slot_state: ReadFenceSlotState::Occupied {
                summary: Some(Box::new(record_summary())),
            },
        })),
    ));
    cells
}

fn request_cell(
    operation: &'static str,
    command_payload_bytes: Option<usize>,
    body: RecorderRequestBody,
) -> SemanticCell {
    SemanticCell {
        id: cell_id(operation, "request", command_payload_bytes),
        operation,
        direction: "request",
        command_payload_bytes,
        value: WireValue::Request(RequestFrame {
            version: WIRE_VERSION,
            request_id: 9001,
            remaining_deadline_ms: 2_000,
            body,
        }),
    }
}

fn response_cell(
    operation: &'static str,
    command_payload_bytes: Option<usize>,
    body: RecorderResponseBody,
) -> SemanticCell {
    SemanticCell {
        id: cell_id(operation, "response", command_payload_bytes),
        operation,
        direction: "response",
        command_payload_bytes,
        value: WireValue::Response(ResponseFrame {
            version: WIRE_VERSION,
            request_id: 9001,
            body,
        }),
    }
}

fn cell_id(operation: &str, direction: &str, payload_bytes: Option<usize>) -> String {
    match payload_bytes {
        Some(bytes) => format!("{operation}.{direction}.command_payload_{bytes}"),
        None => format!("{operation}.{direction}"),
    }
}

fn expected_cell_ids() -> BTreeSet<String> {
    let mut ids = BTreeSet::new();
    for operation in [
        "identity",
        "store_command",
        "fetch_command",
        "record",
        "install_decision_proof",
        "inspect_decision_proof",
        "inspect_record_summary",
        "observe_read_fence",
    ] {
        ids.insert(cell_id(operation, "request", None));
        ids.insert(cell_id(operation, "response", None));
    }
    for (operation, direction) in [
        ("store_command", "request"),
        ("fetch_command", "response"),
        ("record", "request"),
    ] {
        ids.remove(&cell_id(operation, direction, None));
        for bytes in PAYLOAD_BYTES {
            ids.insert(cell_id(operation, direction, Some(bytes)));
        }
    }
    ids
}

fn stored_command(payload_bytes: usize) -> StoredCommand {
    StoredCommand {
        entry_type: EntryType::Command,
        payload: (0..payload_bytes)
            .map(|index| (index.wrapping_mul(31) & 0xff) as u8)
            .collect(),
    }
}

fn proposal() -> Proposal {
    Proposal {
        priority: ProposalPriority(hash_bytes(17)),
        proposer_id: "node-2".into(),
        proposal_id: 23,
        value: Some(AcceptedValue {
            command_hash: hash(42),
            prev_hash: hash(41),
            entry_hash: hash(43),
        }),
    }
}

fn recorder_summaries() -> Vec<RecorderSummary> {
    ["node-1", "node-2"]
        .into_iter()
        .map(|recorder_id| RecorderSummary {
            recorder_id: recorder_id.into(),
            slot: 42,
            step: 2,
            first_current: Some(proposal()),
            aggregate_prior: Some(proposal()),
        })
        .collect()
}

fn decision_proof() -> DecisionProof {
    DecisionProof::Phase2 {
        cluster_id: "bench".into(),
        slot: 42,
        epoch: 7,
        config_id: 11,
        config_digest: hash(11),
        step: 2,
        proposal: proposal(),
        summaries: recorder_summaries(),
    }
}

fn record_request(payload_bytes: usize) -> RecordRequest {
    RecordRequest {
        cluster_id: "bench".into(),
        epoch: 7,
        config_id: 11,
        config_digest: hash(11),
        slot: 42,
        step: 2,
        proposal: proposal(),
        command: Some(stored_command(payload_bytes)),
    }
}

fn record_summary() -> RecordSummary {
    RecordSummary {
        recorder_id: "node-1".into(),
        slot: 42,
        config_id: 11,
        config_digest: hash(11),
        step: 2,
        first_current: Some(proposal()),
        aggregate_prior: Some(proposal()),
        decided: Some(decision_proof()),
    }
}

fn read_fence_request() -> ReadFenceRequest {
    ReadFenceRequest {
        cluster_id: "bench".into(),
        epoch: 7,
        config_id: 11,
        config_digest: hash(11),
        slot: 42,
    }
}

fn hash(seed: usize) -> LogHash {
    LogHash(hash_bytes(seed))
}

fn hash_bytes(seed: usize) -> [u8; 32] {
    std::array::from_fn(|index| seed.wrapping_add(index).wrapping_mul(17) as u8)
}

struct Config {
    warmup: usize,
    iterations: usize,
    candidate_order_offset: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            warmup: 1_000,
            iterations: 10_000,
            candidate_order_offset: 0,
        }
    }
}

impl Config {
    fn parse(args: &[String]) -> Result<Self, String> {
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
                "--iterations" => config.iterations = parse_positive(value, flag)?,
                "--candidate-order-offset" => {
                    config.candidate_order_offset = value
                        .parse()
                        .map_err(|_| format!("{flag} requires a non-negative integer"))?
                }
                _ => return Err(format!("unknown option {flag:?}")),
            }
            index += 2;
        }
        Ok(config)
    }

    fn codecs(&self) -> [Codec; 2] {
        let mut codecs = Codec::ALL;
        codecs.rotate_left(self.candidate_order_offset % 2);
        codecs
    }
}

fn parse_positive(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{flag} requires a positive integer"))
}

fn print_usage() {
    eprintln!(
        "Usage: rhiza-recorder-codec [--warmup N] [--iterations N] \
         [--candidate-order-offset N]"
    );
}

#[derive(Serialize)]
struct PhaseMetric {
    wall_seconds: f64,
    latency_ns_per_operation: f64,
    throughput_operations_per_second: f64,
}

impl PhaseMetric {
    fn new(elapsed: Duration, iterations: usize) -> Self {
        let wall_seconds = elapsed.as_secs_f64();
        Self {
            wall_seconds,
            latency_ns_per_operation: elapsed.as_nanos() as f64 / iterations as f64,
            throughput_operations_per_second: iterations as f64 / wall_seconds,
        }
    }
}

#[derive(Serialize)]
struct Metric {
    codec: &'static str,
    codec_version: &'static str,
    cell_id: String,
    operation: &'static str,
    direction: &'static str,
    command_payload_bytes: Option<usize>,
    serialized_bytes: usize,
    encoded_fnv1a64: String,
    round_trip_verified: bool,
    warmup_iterations: usize,
    measured_iterations: usize,
    encode: PhaseMetric,
    decode_validation_owned_materialization: PhaseMetric,
    encode_decode_total: PhaseMetric,
}

fn measure(
    codec: Codec,
    cell: &SemanticCell,
    warmup: usize,
    iterations: usize,
) -> Result<Metric, String> {
    let encoded = encode(codec, &cell.value)?;
    let network_bytes = encoded.as_slice().to_vec();
    let round_trip_verified = decode(codec, &cell.value, &network_bytes)? == cell.value;
    if !round_trip_verified {
        return Err(format!("{} changed during round trip", cell.id));
    }
    for _ in 0..warmup {
        let bytes = black_box(encode(codec, black_box(&cell.value))?);
        let decoded = decode(codec, &cell.value, black_box(bytes.as_slice()))?;
        black_box(decoded);
    }

    let started = Instant::now();
    for _ in 0..iterations {
        let bytes = encode(codec, black_box(&cell.value))?;
        black_box(bytes);
    }
    let encode_elapsed = started.elapsed();

    let started = Instant::now();
    for _ in 0..iterations {
        let decoded = decode(codec, &cell.value, black_box(&network_bytes))?;
        black_box(decoded);
    }
    let decode_elapsed = started.elapsed();

    let started = Instant::now();
    for _ in 0..iterations {
        let bytes = encode(codec, black_box(&cell.value))?;
        let decoded = decode(codec, &cell.value, black_box(bytes.as_slice()))?;
        black_box((bytes, decoded));
    }
    let total_elapsed = started.elapsed();

    Ok(Metric {
        codec: codec.name(),
        codec_version: codec.version(),
        cell_id: cell.id.clone(),
        operation: cell.operation,
        direction: cell.direction,
        command_payload_bytes: cell.command_payload_bytes,
        serialized_bytes: network_bytes.len(),
        encoded_fnv1a64: format!("{:016x}", fnv1a64(&network_bytes)),
        round_trip_verified,
        warmup_iterations: warmup,
        measured_iterations: iterations,
        encode: PhaseMetric::new(encode_elapsed, iterations),
        decode_validation_owned_materialization: PhaseMetric::new(decode_elapsed, iterations),
        encode_decode_total: PhaseMetric::new(total_elapsed, iterations),
    })
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |digest, byte| {
        (digest ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

#[derive(Serialize)]
struct Environment {
    release_build: bool,
    cargo_profile: &'static str,
    cargo_opt_level: &'static str,
    git_commit: Option<String>,
    git_dirty: Option<bool>,
    rustc: Option<String>,
    os: Option<String>,
    cpu: Option<String>,
    hostname: Option<String>,
}

#[derive(Serialize)]
struct Conditions {
    scope: &'static str,
    wire_fixture: &'static str,
    command_payload_bytes: [usize; 3],
    payload_policy: &'static str,
    operations: [&'static str; 8],
    directions: [&'static str; 2],
    expected_semantic_cells: usize,
    expected_metrics: usize,
    codecs: [&'static str; 2],
    rkyv_features: [&'static str; 4],
    rkyv_input_handling: &'static str,
    optimization_barrier: &'static str,
    decode_definition: &'static str,
    total_definition: &'static str,
    excludes: &'static str,
    warmup_iterations_per_cell: usize,
    measured_iterations_per_phase_per_cell: usize,
    candidate_order_offset: usize,
    candidate_order: [&'static str; 2],
}

#[derive(Serialize)]
struct Report {
    schema_version: u8,
    generated_at_epoch_seconds: f64,
    run_id: String,
    diagnostic_valid: bool,
    diagnostic_blockers: Vec<String>,
    comparison_valid: bool,
    comparison_blockers: Vec<&'static str>,
    invocation: Vec<String>,
    environment: Environment,
    conditions: Conditions,
    metrics: Vec<Metric>,
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

fn environment() -> Environment {
    Environment {
        release_build: is_optimized_release(CARGO_PROFILE, CARGO_OPT_LEVEL),
        cargo_profile: CARGO_PROFILE,
        cargo_opt_level: CARGO_OPT_LEVEL,
        git_commit: command_output("git", &["rev-parse", "HEAD"]),
        git_dirty: git_dirty(),
        rustc: command_output("rustc", &["--version"]),
        os: command_output("uname", &["-a"]),
        cpu: command_output("sysctl", &["-n", "machdep.cpu.brand_string"])
            .or_else(|| command_output("sh", &["-c", "grep -m1 'model name' /proc/cpuinfo"])),
        hostname: command_output("hostname", &[]),
    }
}

fn is_optimized_release(profile: &str, opt_level: &str) -> bool {
    profile == "release" && matches!(opt_level, "1" | "2" | "3" | "s" | "z")
}

fn diagnostic_blockers(metrics: &[Metric], profile: &str, opt_level: &str) -> Vec<String> {
    let expected = expected_cell_ids();
    let actual = metrics
        .iter()
        .map(|metric| metric.cell_id.clone())
        .collect::<BTreeSet<_>>();
    let actual_metric_keys = metrics
        .iter()
        .map(|metric| (metric.codec, metric.cell_id.as_str()))
        .collect::<BTreeSet<_>>();
    let expected_metric_keys = expected
        .iter()
        .flat_map(|cell_id| Codec::ALL.map(move |codec| (codec.name(), cell_id.as_str())))
        .collect::<BTreeSet<_>>();
    let mut blockers = Vec::new();
    if !is_optimized_release(profile, opt_level) {
        blockers.push(format!(
            "benchmark requires Cargo profile=release with optimization enabled; observed profile={profile}, opt-level={opt_level}"
        ));
    }
    if actual != expected {
        blockers.push(
            "semantic cell identities do not exactly match the production-shaped plan".into(),
        );
    }
    if actual_metric_keys != expected_metric_keys || metrics.len() != expected_metric_keys.len() {
        blockers.push("codec/semantic cell metrics are missing or duplicated".into());
    }
    if metrics.iter().any(|metric| !metric.round_trip_verified) {
        blockers.push("one or more full owned round trips failed".into());
    }
    blockers
}

fn run(config: Config, invocation: Vec<String>) -> Result<Report, String> {
    let codecs = config.codecs();
    let cells = semantic_cells();
    let mut metrics = Vec::with_capacity(cells.len() * codecs.len());
    for cell in &cells {
        for codec in codecs {
            metrics.push(measure(codec, cell, config.warmup, config.iterations)?);
        }
    }
    let generated_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();
    let environment = environment();
    let diagnostic_blockers = diagnostic_blockers(
        &metrics,
        environment.cargo_profile,
        environment.cargo_opt_level,
    );
    Ok(Report {
        schema_version: 2,
        generated_at_epoch_seconds: generated_at,
        run_id: format!(
            "{}-{}-{}",
            generated_at,
            process::id(),
            config.candidate_order_offset
        ),
        diagnostic_valid: diagnostic_blockers.is_empty(),
        diagnostic_blockers,
        comparison_valid: false,
        comparison_blockers: vec![
            "single raw run; repeat paired runs with alternating candidate order",
            "codec-only diagnostic; production promotion requires durable QuePaxa evidence",
        ],
        invocation,
        environment,
        conditions: Conditions {
            scope: "in-process codec-only production-shaped Recorder TCP request and response DTOs",
            wire_fixture: "private benchmark DTOs mirror recorder_tcp RequestFrame, RecorderRequestBody, ResponseFrame, RecorderResponseBody, RpcResult, and nested public QuePaxa fields; no benchmark wrapper bytes are serialized",
            command_payload_bytes: PAYLOAD_BYTES,
            payload_policy: "only StoreCommand requests, successful FetchCommand responses, and Record requests carry 0/128/4096-byte StoredCommand payloads; every other semantic request/response is measured exactly once without a payload label",
            operations: [
                "identity",
                "store_command",
                "fetch_command",
                "record",
                "install_decision_proof",
                "inspect_decision_proof",
                "inspect_record_summary",
                "observe_read_fence",
            ],
            directions: ["request", "response"],
            expected_semantic_cells: expected_cell_ids().len(),
            expected_metrics: expected_cell_ids().len() * Codec::ALL.len(),
            codecs: ["postcard", "rkyv"],
            rkyv_features: ["std", "bytecheck", "little_endian", "pointer_width_32"],
            rkyv_input_handling: "every decode copies network-style &[u8] into rkyv::AlignedVec before checked rkyv::from_bytes; no zero-copy claim",
            optimization_barrier: "black_box consumes each complete encoded allocation and each complete fully owned decoded value; full round-trip equality and encoded FNV-1a digests are computed outside timed phases",
            decode_definition: "Postcard full owned decode; rkyv alignment copy plus structural validation plus full owned materialization",
            total_definition: "one fresh encode followed by the candidate's complete decode path",
            excludes: "transport framing and I/O, authentication, RecorderRpc adapter/domain conversion, quorum, persistence, fsync, materializer, remote network, and commits/second",
            warmup_iterations_per_cell: config.warmup,
            measured_iterations_per_phase_per_cell: config.iterations,
            candidate_order_offset: config.candidate_order_offset,
            candidate_order: [codecs[0].name(), codecs[1].name()],
        },
        metrics,
    })
}

fn main() {
    let invocation = env::args().collect::<Vec<_>>();
    let config = Config::parse(&invocation).unwrap_or_else(|error| {
        eprintln!("configuration error: {error}");
        print_usage();
        process::exit(2);
    });
    let report = run(config, invocation).unwrap_or_else(|error| {
        eprintln!("benchmark failed: {error}");
        process::exit(1);
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_plan_labels_payload_only_where_production_can_carry_command_bytes() {
        let cells = semantic_cells();
        let ids = cells
            .iter()
            .map(|cell| cell.id.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(ids, expected_cell_ids());
        assert_eq!(cells.len(), 22);
        assert!(cells
            .iter()
            .filter(|cell| cell.command_payload_bytes.is_some())
            .all(|cell| matches!(
                (cell.operation, cell.direction),
                ("store_command", "request")
                    | ("fetch_command", "response")
                    | ("record", "request")
            )));
    }

    #[test]
    fn both_codecs_round_trip_every_complete_production_shaped_semantic_cell() {
        for cell in semantic_cells() {
            for codec in Codec::ALL {
                let bytes = encode(codec, &cell.value).unwrap();
                assert_eq!(
                    decode(codec, &cell.value, bytes.as_slice()).unwrap(),
                    cell.value,
                    "{} via {}",
                    cell.id,
                    codec.name()
                );
            }
        }
    }

    #[test]
    fn rkyv_decode_aligns_network_input_before_checked_owned_materialization() {
        let cell = semantic_cells()
            .into_iter()
            .find(|cell| cell.id == "record.request.command_payload_128")
            .unwrap();
        let bytes = encode(Codec::Rkyv, &cell.value).unwrap();
        let mut deliberately_offset = vec![0];
        deliberately_offset.extend_from_slice(bytes.as_slice());
        assert_eq!(
            decode(Codec::Rkyv, &cell.value, &deliberately_offset[1..]).unwrap(),
            cell.value
        );
    }

    #[test]
    fn rkyv_decode_rejects_a_truncated_archive() {
        let cell = semantic_cells()
            .into_iter()
            .find(|cell| cell.id == "install_decision_proof.request")
            .unwrap();
        let bytes = encode(Codec::Rkyv, &cell.value).unwrap();
        assert!(decode(
            Codec::Rkyv,
            &cell.value,
            &bytes.as_slice()[..bytes.as_slice().len() - 1]
        )
        .is_err());
    }

    #[test]
    fn diagnostic_validity_requires_release_unique_cells_and_verified_round_trips() {
        let metrics = expected_cell_ids()
            .into_iter()
            .flat_map(|cell_id| {
                Codec::ALL.map(move |codec| Metric {
                    codec: codec.name(),
                    codec_version: codec.version(),
                    cell_id: cell_id.clone(),
                    operation: "fixture",
                    direction: "request",
                    command_payload_bytes: None,
                    serialized_bytes: 1,
                    encoded_fnv1a64: "0000000000000000".into(),
                    round_trip_verified: true,
                    warmup_iterations: 1,
                    measured_iterations: 1,
                    encode: PhaseMetric::new(Duration::from_nanos(1), 1),
                    decode_validation_owned_materialization: PhaseMetric::new(
                        Duration::from_nanos(1),
                        1,
                    ),
                    encode_decode_total: PhaseMetric::new(Duration::from_nanos(1), 1),
                })
            })
            .collect::<Vec<_>>();
        assert!(diagnostic_blockers(&metrics, "release", "3").is_empty());
        assert!(diagnostic_blockers(&metrics, "dev", "3")
            .iter()
            .any(|blocker| blocker.contains("release")));
        assert!(diagnostic_blockers(&metrics, "release", "0")
            .iter()
            .any(|blocker| blocker.contains("optimization")));
    }

    #[test]
    fn optimized_release_check_rejects_dev_even_without_debug_assertions() {
        assert!(is_optimized_release("release", "3"));
        assert!(is_optimized_release("release", "s"));
        assert!(!is_optimized_release("debug", "3"));
        assert!(!is_optimized_release("dev", "3"));
        assert!(!is_optimized_release("dev", "0"));
        assert!(!is_optimized_release("release", "0"));
        assert!(!is_optimized_release("release", "unknown"));
    }

    #[test]
    fn candidate_order_offset_rotates_the_pair() {
        let config = Config {
            candidate_order_offset: 1,
            ..Config::default()
        };
        assert_eq!(config.codecs(), [Codec::Rkyv, Codec::Postcard]);
    }
}
