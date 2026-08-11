use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use rhiza_quepaxa::{
    AcceptedValue, EntryType, LogHash, Membership, Proposal, ProposalPriority, RecordRequest,
    RecorderFileStore, StoredCommand,
};
use serde::Serialize;

const CLUSTER_ID: &str = "recorder-sync-bench";
const RECORDER_ID: &str = "n1";
const EPOCH: u64 = 1;
const CONFIG_ID: u64 = 1;
const DEFAULT_OPERATIONS: usize = 500;
const DEFAULT_WARMUP: usize = 100;
const DEFAULT_PAYLOAD_BYTES: usize = 128;
const MIN_PAYLOAD_BYTES: usize = 2;
const MAX_PAYLOAD_BYTES: usize = 4 * 1024;
const WAL_HARD_FRAME_LIMIT: usize = 1_024;
const WAL_SOFT_BYTE_LIMIT: u64 = 16 * 1024 * 1024;
const WAL_PREFIX_LEN: usize = 4 + 2 + 8;
const WAL_DIGEST_LEN: usize = 32;
const WAL_CHAIN_FIELDS_LEN: usize = 8 + 8 + 32;
const TWO_ROTATION_OPERATIONS: usize = WAL_HARD_FRAME_LIMIT * 2 + 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum CommandMode {
    Inline,
    PreStored,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    operations: usize,
    warmup: usize,
    payload_bytes: usize,
    label: String,
    root: Option<PathBuf>,
    keep: bool,
    command_mode: CommandMode,
    checkpoint_diagnostic: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            operations: DEFAULT_OPERATIONS,
            warmup: DEFAULT_WARMUP,
            payload_bytes: DEFAULT_PAYLOAD_BYTES,
            label: "native".into(),
            root: None,
            keep: false,
            command_mode: CommandMode::Inline,
            checkpoint_diagnostic: false,
        }
    }
}

impl Config {
    fn parse_from(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut config = Self::default();
        let mut args = args.into_iter();
        let mut operations_explicit = false;
        let mut warmup_explicit = false;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--operations" => {
                    config.operations = parse_usize("--operations", args.next())?;
                    operations_explicit = true;
                }
                "--warmup" => {
                    config.warmup = parse_usize("--warmup", args.next())?;
                    warmup_explicit = true;
                }
                "--payload-bytes" => {
                    config.payload_bytes = parse_usize("--payload-bytes", args.next())?;
                }
                "--label" => {
                    config.label = args
                        .next()
                        .ok_or_else(|| "--label requires a value".to_owned())?;
                }
                "--root" => {
                    config.root = Some(PathBuf::from(
                        args.next()
                            .ok_or_else(|| "--root requires a path".to_owned())?,
                    ));
                }
                "--keep" => config.keep = true,
                "--command-mode" => {
                    config.command_mode = match args.next().as_deref() {
                        Some("inline") => CommandMode::Inline,
                        Some("pre-stored") => CommandMode::PreStored,
                        Some(value) => {
                            return Err(format!(
                                "invalid --command-mode {value:?}; expected inline or pre-stored"
                            ));
                        }
                        None => return Err("--command-mode requires a value".into()),
                    };
                }
                "--checkpoint-diagnostic" => config.checkpoint_diagnostic = true,
                "--help" | "-h" => return Err(usage()),
                other => return Err(format!("unknown argument {other:?}\n{}", usage())),
            }
        }
        if config.checkpoint_diagnostic {
            if warmup_explicit && config.warmup != 0 {
                return Err("--checkpoint-diagnostic requires --warmup 0".into());
            }
            if operations_explicit && config.operations != TWO_ROTATION_OPERATIONS {
                return Err(format!(
                    "--checkpoint-diagnostic requires --operations {TWO_ROTATION_OPERATIONS}"
                ));
            }
            config.warmup = 0;
            config.operations = TWO_ROTATION_OPERATIONS;
        }
        if config.operations == 0 {
            return Err("--operations must be greater than zero".into());
        }
        if config.payload_bytes < MIN_PAYLOAD_BYTES {
            return Err(format!(
                "--payload-bytes must be at least {MIN_PAYLOAD_BYTES}; two bytes identify each benchmark operation"
            ));
        }
        if config.payload_bytes > MAX_PAYLOAD_BYTES {
            return Err(format!(
                "--payload-bytes must not exceed {MAX_PAYLOAD_BYTES}"
            ));
        }
        let total = config
            .warmup
            .checked_add(config.operations)
            .ok_or_else(|| "warmup + operations overflowed".to_owned())?;
        if total > usize::from(u16::MAX) {
            return Err(format!(
                "warmup + operations must not exceed {}; payload markers are u16",
                u16::MAX
            ));
        }
        Ok(config)
    }
}

fn parse_usize(flag: &str, value: Option<String>) -> Result<usize, String> {
    value
        .ok_or_else(|| format!("{flag} requires a value"))?
        .parse::<usize>()
        .map_err(|error| format!("invalid {flag}: {error}"))
}

fn usage() -> String {
    format!(
        "usage: recorder_sync_bench [--operations N] [--warmup N] [--payload-bytes N] [--label NAME] [--root PATH] [--keep] [--command-mode inline|pre-stored] [--checkpoint-diagnostic]\n\n  --payload-bytes N\n      exact equal command payload size; at least 2 bytes identify each operation\n  --command-mode inline|pre-stored\n      inline is the default and includes distinct inline command persistence in each timed record;\n      pre-stored stores every distinct command before timing and omits it from timed requests\n  --checkpoint-diagnostic\n      forces --warmup 0 --operations {}; operations {} and {} cross two 1,024-frame boundaries,\n      must perform exactly two checkpoints before generation-2 and generation-3 frames, and\n      must reopen through the production decoder",
        TWO_ROTATION_OPERATIONS,
        WAL_HARD_FRAME_LIMIT + 1,
        TWO_ROTATION_OPERATIONS,
    )
}

#[derive(Debug, Serialize)]
struct Platform {
    os: &'static str,
    arch: &'static str,
    family: &'static str,
    ld_preload: bool,
}

#[derive(Debug, Serialize)]
struct LatencyNs {
    p50: Option<u64>,
    p95: Option<u64>,
    p99: Option<u64>,
    max: Option<u64>,
}

#[derive(Debug, Serialize)]
struct OperationObservation {
    operation: usize,
    call_elapsed_ns: u64,
    completed: bool,
    checkpoint_observed: bool,
}

#[derive(Debug, Serialize)]
struct CheckpointEvent {
    operation: usize,
    generation: u64,
}

struct PreparedOperation {
    command: StoredCommand,
    request: RecordRequest,
}

#[derive(Debug, Serialize)]
struct WalObservation {
    bytes: u64,
    frames: usize,
    generations: Vec<u64>,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    checkpoint_generation: u64,
    checkpoint_through_sequence: u64,
    checkpoints_observed: usize,
    checkpoint_avoided_observed: bool,
}

#[derive(Debug, Serialize)]
struct Report {
    benchmark: &'static str,
    sync_variant: String,
    command_mode: CommandMode,
    checkpoint_diagnostic: bool,
    operations: usize,
    warmup: usize,
    payload_bytes: usize,
    completed: usize,
    errors: usize,
    elapsed_ns: u64,
    ops_per_second: f64,
    latency_scope: &'static str,
    latency_ns: LatencyNs,
    checkpoint_boundary_operations: Vec<OperationObservation>,
    checkpoint_events: Vec<CheckpointEvent>,
    wal: WalObservation,
    platform: Platform,
}

fn main() {
    let config = match Config::parse_from(env::args().skip(1)) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            process::exit(2);
        }
    };
    match run(config) {
        Ok((report, failed)) => {
            println!(
                "{}",
                serde_json::to_string(&report).expect("benchmark report must serialize")
            );
            if failed {
                process::exit(1);
            }
        }
        Err(message) => {
            eprintln!("recorder sync benchmark failed: {message}");
            process::exit(1);
        }
    }
}

fn run(config: Config) -> Result<(Report, bool), String> {
    let generated_root = config.root.is_none();
    let root = config.root.clone().unwrap_or_else(unique_root);
    if root.exists() {
        return Err(format!("benchmark root already exists: {}", root.display()));
    }

    let result = run_at_root(&config, &root);
    if generated_root && !config.keep {
        let _ = fs::remove_dir_all(&root);
    }
    result
}

fn run_at_root(config: &Config, root: &Path) -> Result<(Report, bool), String> {
    let membership = Membership::new(["n1", "n2", "n3"]).map_err(|error| error.to_string())?;
    let config_digest = membership.digest();
    let expected_membership = membership.clone();
    let store = RecorderFileStore::new_with_membership(
        root,
        RECORDER_ID,
        CLUSTER_ID,
        EPOCH,
        CONFIG_ID,
        membership,
    )
    .map_err(|error| error.to_string())?;
    let total = config.warmup + config.operations;
    let prepared = prepare_operations(
        config_digest,
        config.payload_bytes,
        total,
        config.command_mode,
    );
    if config.command_mode == CommandMode::PreStored {
        for operation in &prepared {
            store
                .store_command(operation.command.hash(), operation.command.clone())
                .map_err(|error| format!("pre-store setup failed: {error}"))?;
        }
    }
    let mut requests: Vec<_> = prepared
        .into_iter()
        .map(|operation| operation.request)
        .collect();
    let measured_requests = requests.split_off(config.warmup);

    for (index, request) in requests.into_iter().enumerate() {
        store
            .record_proposal(request)
            .map_err(|error| format!("warmup operation {index} failed: {error}"))?;
    }

    let started = Instant::now();
    let mut latencies = Vec::with_capacity(config.operations);
    let mut boundary_operations = Vec::with_capacity(6);
    let mut errors = 0usize;
    for (index, request) in measured_requests.into_iter().enumerate() {
        let operation = config.warmup + index + 1;
        let operation_started = Instant::now();
        let completed = store.record_proposal(request).is_ok();
        let call_elapsed_ns = duration_ns(operation_started.elapsed().as_nanos());
        if completed {
            latencies.push(call_elapsed_ns);
        } else {
            errors += 1;
        }
        if [WAL_HARD_FRAME_LIMIT, WAL_HARD_FRAME_LIMIT * 2]
            .into_iter()
            .any(|boundary| (boundary - 1..=boundary + 1).contains(&operation))
        {
            boundary_operations.push(OperationObservation {
                operation,
                call_elapsed_ns,
                completed,
                checkpoint_observed: false,
            });
        }
    }
    let elapsed_ns = duration_ns(started.elapsed().as_nanos());
    latencies.sort_unstable();
    let completed = config.operations - errors;
    let ops_per_second = if elapsed_ns == 0 {
        0.0
    } else {
        completed as f64 * 1_000_000_000.0 / elapsed_ns as f64
    };
    drop(store);
    let wal = observe_reopenable_wal(root, expected_membership, total)?;
    let diagnostic_passed = checkpoint_diagnostic_passed(&wal);
    let checkpoint_events = if config.checkpoint_diagnostic && diagnostic_passed {
        vec![
            CheckpointEvent {
                operation: WAL_HARD_FRAME_LIMIT + 1,
                generation: 2,
            },
            CheckpointEvent {
                operation: TWO_ROTATION_OPERATIONS,
                generation: 3,
            },
        ]
    } else {
        Vec::new()
    };
    for observation in &mut boundary_operations {
        observation.checkpoint_observed = checkpoint_events
            .iter()
            .any(|event| event.operation == observation.operation);
    }
    let report = Report {
        benchmark: "recorder_wal_record",
        sync_variant: config.label.clone(),
        command_mode: config.command_mode,
        checkpoint_diagnostic: config.checkpoint_diagnostic,
        operations: config.operations,
        warmup: config.warmup,
        payload_bytes: config.payload_bytes,
        completed,
        errors,
        elapsed_ns,
        ops_per_second,
        latency_scope: "successful_calls_only",
        latency_ns: LatencyNs {
            p50: percentile(&latencies, 50),
            p95: percentile(&latencies, 95),
            p99: percentile(&latencies, 99),
            max: latencies.last().copied(),
        },
        checkpoint_boundary_operations: boundary_operations,
        checkpoint_events,
        wal,
        platform: Platform {
            os: env::consts::OS,
            arch: env::consts::ARCH,
            family: env::consts::FAMILY,
            ld_preload: env::var_os("LD_PRELOAD").is_some(),
        },
    };
    Ok((
        report,
        errors != 0 || (config.checkpoint_diagnostic && !diagnostic_passed),
    ))
}

fn prepare_operations(
    config_digest: LogHash,
    payload_bytes: usize,
    total: usize,
    command_mode: CommandMode,
) -> Vec<PreparedOperation> {
    (1..=total)
        .map(|operation| {
            let mut payload = vec![0x5a; payload_bytes];
            let marker = u16::try_from(operation)
                .expect("benchmark operation count is capped at u16::MAX")
                .to_be_bytes();
            payload[..marker.len()].copy_from_slice(&marker);
            let command = StoredCommand::new(EntryType::Command, payload);
            let slot = u64::try_from(operation).expect("benchmark operation count fits u64");
            let request = record_request(config_digest, slot, &command, command_mode);
            PreparedOperation { command, request }
        })
        .collect()
}

fn record_request(
    config_digest: LogHash,
    slot: u64,
    command: &StoredCommand,
    command_mode: CommandMode,
) -> RecordRequest {
    let value =
        AcceptedValue::from_command(CLUSTER_ID, slot, EPOCH, CONFIG_ID, LogHash::ZERO, command);
    RecordRequest {
        cluster_id: CLUSTER_ID.into(),
        epoch: EPOCH,
        config_id: CONFIG_ID,
        config_digest,
        slot,
        step: 4,
        proposal: Proposal::new(ProposalPriority::MAX, "benchmark-proposer", slot, value),
        command: (command_mode == CommandMode::Inline).then(|| command.clone()),
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> Option<u64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (sorted.len() * percentile).div_ceil(100);
    Some(sorted[rank.saturating_sub(1).min(sorted.len() - 1)])
}

fn checkpoint_diagnostic_passed(wal: &WalObservation) -> bool {
    wal.checkpoints_observed == 2
        && wal.checkpoint_generation == 3
        && wal.checkpoint_through_sequence == (WAL_HARD_FRAME_LIMIT * 2) as u64
        && wal.frames == 1
        && wal.generations == [3]
        && wal.first_sequence == Some(TWO_ROTATION_OPERATIONS as u64)
        && wal.last_sequence == Some(TWO_ROTATION_OPERATIONS as u64)
}

fn observe_reopenable_wal(
    root: &Path,
    membership: Membership,
    attempted_frames: usize,
) -> Result<WalObservation, String> {
    let wal = observe_wal(
        &root.join("recorder.wal"),
        &root.join("recorded-head.rec"),
        attempted_frames,
    )?;
    let reopened = RecorderFileStore::new_with_membership(
        root,
        RECORDER_ID,
        CLUSTER_ID,
        EPOCH,
        CONFIG_ID,
        membership,
    )
    .map_err(|error| format!("failed to reopen benchmark recorder: {error}"))?;
    drop(reopened);
    Ok(wal)
}

fn observe_wal(
    path: &Path,
    recorded_head_path: &Path,
    attempted_frames: usize,
) -> Result<WalObservation, String> {
    let (checkpoint_generation, checkpoint_through_sequence) =
        observe_checkpoint(recorded_head_path)?;
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    let mut offset = 0usize;
    let mut frames = 0usize;
    let mut generations = Vec::new();
    let mut first_sequence = None;
    let mut last_sequence = None;
    let mut expected_sequence = checkpoint_through_sequence
        .checked_add(1)
        .ok_or_else(|| "WAL checkpoint sequence exhausted after benchmark".to_owned())?;
    let mut expected_prev_digest = LogHash::ZERO;
    while offset < bytes.len() {
        let prefix = bytes
            .get(offset..offset.saturating_add(WAL_PREFIX_LEN))
            .ok_or_else(|| "truncated WAL frame header after benchmark".to_owned())?;
        if prefix.get(..4) != Some(b"QWAL") || prefix.get(4..6) != Some(&1u16.to_be_bytes()) {
            return Err("unexpected WAL frame identity after benchmark".into());
        }
        let frame_len = usize::try_from(u64::from_be_bytes(
            prefix[6..14]
                .try_into()
                .map_err(|_| "invalid WAL frame length".to_owned())?,
        ))
        .map_err(|_| "WAL frame length overflow".to_owned())?;
        if frame_len < WAL_PREFIX_LEN + WAL_CHAIN_FIELDS_LEN + WAL_DIGEST_LEN {
            return Err("invalid WAL frame extent after benchmark".into());
        }
        let end = offset
            .checked_add(frame_len)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| "invalid WAL frame extent after benchmark".to_owned())?;
        let digest_offset = end - WAL_DIGEST_LEN;
        let expected_digest = LogHash::digest(&[&bytes[offset..digest_offset]]);
        if bytes.get(digest_offset..end) != Some(expected_digest.as_bytes()) {
            return Err("WAL frame checksum mismatch after benchmark".into());
        }
        let chain_end = offset
            .checked_add(WAL_PREFIX_LEN + WAL_CHAIN_FIELDS_LEN)
            .ok_or_else(|| "WAL chain fields offset overflow after benchmark".to_owned())?;
        let chain_prefix = bytes
            .get(offset..chain_end)
            .ok_or_else(|| "truncated WAL chain fields after benchmark".to_owned())?;
        let generation = u64::from_be_bytes(
            chain_prefix[14..22]
                .try_into()
                .map_err(|_| "invalid WAL generation".to_owned())?,
        );
        let sequence = u64::from_be_bytes(
            chain_prefix[22..30]
                .try_into()
                .map_err(|_| "invalid WAL sequence".to_owned())?,
        );
        let prev_digest = &chain_prefix[30..62];
        if generation != checkpoint_generation
            || sequence != expected_sequence
            || prev_digest != expected_prev_digest.as_bytes()
        {
            return Err(
                "WAL generation, sequence, or digest chain mismatch after benchmark".into(),
            );
        }
        if generations.last() != Some(&generation) {
            generations.push(generation);
        }
        first_sequence.get_or_insert(sequence);
        last_sequence = Some(sequence);
        expected_sequence = sequence
            .checked_add(1)
            .ok_or_else(|| "WAL sequence exhausted after benchmark".to_owned())?;
        expected_prev_digest = expected_digest;
        frames += 1;
        offset = end;
    }
    let byte_count = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    let checkpoint_avoided_observed = attempted_frames < WAL_HARD_FRAME_LIMIT
        && byte_count < WAL_SOFT_BYTE_LIMIT
        && frames == attempted_frames
        && checkpoint_generation == 1
        && checkpoint_through_sequence == 0
        && generations == [checkpoint_generation]
        && first_sequence == Some(1)
        && last_sequence == Some(attempted_frames as u64);
    let checkpoints_observed = checkpoint_generation
        .saturating_sub(1)
        .try_into()
        .unwrap_or(usize::MAX);
    Ok(WalObservation {
        bytes: byte_count,
        frames,
        generations,
        first_sequence,
        last_sequence,
        checkpoint_generation,
        checkpoint_through_sequence,
        checkpoints_observed,
        checkpoint_avoided_observed,
    })
}

fn observe_checkpoint(path: &Path) -> Result<(u64, u64), String> {
    let bytes = fs::read(path).map_err(|error| error.to_string())?;
    let digest_offset = bytes
        .len()
        .checked_sub(32)
        .ok_or_else(|| "truncated recorder head after benchmark".to_owned())?;
    let expected_digest = LogHash::digest(&[&bytes[..digest_offset]]);
    if bytes.get(digest_offset..) != Some(expected_digest.as_bytes()) {
        return Err("recorder head checksum mismatch after benchmark".into());
    }
    let body = &bytes[..digest_offset];
    if body.get(..4) != Some(b"QRHD") || body.get(4..6) != Some(&3u16.to_be_bytes()) {
        return Err("unexpected recorder head identity after benchmark".into());
    }
    let cluster_len = usize::from(u16::from_be_bytes(
        body.get(6..8)
            .ok_or_else(|| "truncated recorder head cluster length".to_owned())?
            .try_into()
            .map_err(|_| "invalid recorder head cluster length".to_owned())?,
    ));
    let mut cursor = 8usize
        .checked_add(cluster_len)
        .ok_or_else(|| "recorder head cluster length overflow".to_owned())?;
    if body.get(8..cursor) != Some(CLUSTER_ID.as_bytes()) {
        return Err("unexpected recorder head cluster after benchmark".into());
    }
    let epoch = read_observed_u64(body, &mut cursor, "recorder head epoch")?;
    let config_id = read_observed_u64(body, &mut cursor, "recorder head configuration")?;
    if epoch != EPOCH || config_id != CONFIG_ID {
        return Err("unexpected recorder head configuration after benchmark".into());
    }
    skip_observed(body, &mut cursor, 32, "recorder head configuration digest")?;
    let provenance = *body
        .get(cursor)
        .ok_or_else(|| "truncated recorder head provenance".to_owned())?;
    cursor += 1;
    let provenance_len = match provenance {
        0 => 0,
        1 => 8,
        2 => 8 + 32 + 8 + 32,
        _ => return Err("invalid recorder head provenance after benchmark".into()),
    };
    skip_observed(
        body,
        &mut cursor,
        provenance_len,
        "recorder head provenance",
    )?;
    let generation = read_observed_u64(body, &mut cursor, "recorder head WAL generation")?;
    let through_sequence =
        read_observed_u64(body, &mut cursor, "recorder head WAL through-sequence")?;
    if generation == 0 {
        return Err("recorder head has zero WAL generation after benchmark".into());
    }
    Ok((generation, through_sequence))
}

fn read_observed_u64(bytes: &[u8], cursor: &mut usize, field: &str) -> Result<u64, String> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| format!("{field} offset overflow"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| format!("truncated {field}"))?;
    *cursor = end;
    Ok(u64::from_be_bytes(
        value.try_into().map_err(|_| format!("invalid {field}"))?,
    ))
}

fn skip_observed(bytes: &[u8], cursor: &mut usize, len: usize, field: &str) -> Result<(), String> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| format!("{field} offset overflow"))?;
    bytes
        .get(*cursor..end)
        .ok_or_else(|| format!("truncated {field}"))?;
    *cursor = end;
    Ok(())
}

fn duration_ns(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn unique_root() -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    env::temp_dir().join(format!(
        "rhiza-recorder-sync-bench-{}-{timestamp}",
        process::id()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_accepts_explicit_benchmark_shape() {
        let config = Config::parse_from(
            [
                "--operations",
                "700",
                "--warmup",
                "200",
                "--payload-bytes",
                "256",
                "--label",
                "fsync-preload",
                "--keep",
            ]
            .into_iter()
            .map(String::from),
        )
        .unwrap();

        assert_eq!(config.operations, 700);
        assert_eq!(config.warmup, 200);
        assert_eq!(config.payload_bytes, 256);
        assert_eq!(config.label, "fsync-preload");
        assert!(config.keep);
    }

    #[test]
    fn parser_enables_exact_checkpoint_boundary_diagnostic_defaults() {
        let config = Config::parse_from(
            ["--checkpoint-diagnostic", "--command-mode", "pre-stored"]
                .into_iter()
                .map(String::from),
        )
        .unwrap();

        assert_eq!(config.operations, TWO_ROTATION_OPERATIONS);
        assert_eq!(config.warmup, 0);
        assert_eq!(config.command_mode, CommandMode::PreStored);
        assert!(config.checkpoint_diagnostic);
    }

    #[test]
    fn parser_rejects_checkpoint_diagnostic_with_hidden_warmup() {
        let error = Config::parse_from(
            ["--checkpoint-diagnostic", "--warmup", "1"]
                .into_iter()
                .map(String::from),
        )
        .unwrap_err();

        assert!(error.contains("requires --warmup 0"));
    }

    #[test]
    fn help_describes_command_mode_and_checkpoint_measurement_scope() {
        let help = usage();

        assert!(help.contains("inline is the default"));
        assert!(help.contains("pre-stored stores every distinct command before timing"));
        assert!(help.contains("forces --warmup 0 --operations 2049"));
        assert!(help.contains("two checkpoints before generation-2 and generation-3 frames"));
        assert!(help.contains("reopen through the production decoder"));
    }

    #[test]
    fn checkpoint_diagnostic_fails_when_boundary_invariants_are_not_observed() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("short-diagnostic");
        let config = Config {
            operations: 1,
            warmup: 0,
            checkpoint_diagnostic: true,
            ..Config::default()
        };

        let (report, failed) = run_at_root(&config, &root).unwrap();

        assert!(failed);
        assert!(report.checkpoint_events.is_empty());
        assert_eq!(report.wal.checkpoint_generation, 1);
        assert_eq!(report.wal.checkpoint_through_sequence, 0);
    }

    #[test]
    fn checkpoint_diagnostic_gates_two_rotation_boundaries() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("two-rotation-diagnostic");
        let config = Config {
            operations: TWO_ROTATION_OPERATIONS,
            warmup: 0,
            payload_bytes: 2,
            command_mode: CommandMode::PreStored,
            checkpoint_diagnostic: true,
            ..Config::default()
        };

        let (report, failed) = run_at_root(&config, &root).unwrap();

        assert!(!failed);
        assert_eq!(report.checkpoint_events.len(), 2);
        assert_eq!(report.wal.checkpoints_observed, 2);
        assert_eq!(report.wal.checkpoint_generation, 3);
        assert_eq!(report.wal.checkpoint_through_sequence, 2_048);
        assert!(report
            .checkpoint_boundary_operations
            .iter()
            .any(
                |observation| observation.operation == TWO_ROTATION_OPERATIONS
                    && observation.checkpoint_observed
            ));
    }

    #[test]
    fn command_mode_controls_whether_record_contains_inline_command() {
        let command = StoredCommand::new(EntryType::Command, b"payload".to_vec());

        let inline = record_request(LogHash::ZERO, 1, &command, CommandMode::Inline);
        let pre_stored = record_request(LogHash::ZERO, 1, &command, CommandMode::PreStored);

        assert_eq!(inline.command, Some(command));
        assert_eq!(pre_stored.command, None);
    }

    #[test]
    fn prepared_operations_use_equal_sized_distinct_commands() {
        let prepared = prepare_operations(
            LogHash::ZERO,
            2,
            WAL_HARD_FRAME_LIMIT + 1,
            CommandMode::Inline,
        );
        let mut hashes: Vec<_> = prepared
            .iter()
            .map(|operation| operation.command.hash())
            .collect();
        let payload_lengths: Vec<_> = prepared
            .iter()
            .map(|operation| operation.command.payload.len())
            .collect();

        hashes.sort_unstable_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        hashes.dedup();
        assert_eq!(hashes.len(), WAL_HARD_FRAME_LIMIT + 1);
        assert!(payload_lengths.iter().all(|length| *length == 2));
        assert!(prepared
            .iter()
            .all(|operation| { operation.request.command.as_ref() == Some(&operation.command) }));
    }

    #[test]
    fn pre_stored_run_prepares_every_distinct_command_before_measurement() {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("pre-stored-distinct");
        let config = Config {
            operations: 3,
            warmup: 1,
            payload_bytes: 2,
            command_mode: CommandMode::PreStored,
            ..Config::default()
        };

        let (report, failed) = run_at_root(&config, &root).unwrap();

        assert!(!failed);
        assert_eq!(report.completed, 3);
        assert_eq!(report.errors, 0);
        assert_eq!(report.wal.frames, 4);
    }

    #[test]
    fn parser_allows_runs_that_cross_the_checkpoint_boundary() {
        let config = Config::parse_from(
            ["--operations", "901", "--warmup", "100"]
                .into_iter()
                .map(String::from),
        )
        .unwrap();

        assert_eq!(config.operations, 901);
        assert_eq!(config.warmup, 100);
    }

    #[test]
    fn parser_requires_enough_payload_bytes_for_distinct_operations() {
        let error =
            Config::parse_from(["--payload-bytes", "1"].into_iter().map(String::from)).unwrap_err();

        assert!(error.contains("at least 2"));
    }

    #[test]
    fn report_serializes_as_json() {
        let report = Report {
            benchmark: "recorder_wal_record",
            sync_variant: "native".into(),
            command_mode: CommandMode::Inline,
            checkpoint_diagnostic: false,
            operations: 2,
            warmup: 1,
            payload_bytes: 128,
            completed: 2,
            errors: 0,
            elapsed_ns: 100,
            ops_per_second: 20_000_000.0,
            latency_scope: "successful_calls_only",
            latency_ns: LatencyNs {
                p50: Some(40),
                p95: Some(60),
                p99: Some(60),
                max: Some(60),
            },
            checkpoint_boundary_operations: Vec::new(),
            checkpoint_events: Vec::new(),
            wal: WalObservation {
                bytes: 200,
                frames: 3,
                generations: vec![1],
                first_sequence: Some(1),
                last_sequence: Some(3),
                checkpoint_generation: 1,
                checkpoint_through_sequence: 0,
                checkpoints_observed: 0,
                checkpoint_avoided_observed: true,
            },
            platform: Platform {
                os: "linux",
                arch: "x86_64",
                family: "unix",
                ld_preload: false,
            },
        };

        let value = serde_json::to_value(report).unwrap();
        assert_eq!(value["operations"], 2);
        assert_eq!(value["command_mode"], "inline");
        assert_eq!(value["latency_ns"]["p99"], 60);
        assert_eq!(value["latency_ns"]["max"], 60);
        assert_eq!(value["latency_scope"], "successful_calls_only");
        assert_eq!(value["wal"]["checkpoint_avoided_observed"], true);
        assert_eq!(value["wal"]["checkpoint_generation"], 1);
        assert_eq!(value["wal"]["checkpoint_through_sequence"], 0);
        assert_eq!(value["platform"]["os"], "linux");
    }

    #[test]
    fn empty_success_set_has_no_latency_percentiles() {
        assert_eq!(percentile(&[], 99), None);
    }

    #[test]
    fn wal_observation_rejects_header_only_frame() {
        let root = tempfile::tempdir().unwrap();
        write_real_wal(root.path(), 1);
        let path = root.path().join("recorder.wal");
        let mut frame = Vec::from(b"QWAL".as_slice());
        frame.extend_from_slice(&1u16.to_be_bytes());
        frame.extend_from_slice(&30u64.to_be_bytes());
        frame.extend_from_slice(&2u64.to_be_bytes());
        frame.extend_from_slice(&1025u64.to_be_bytes());
        fs::write(&path, frame).unwrap();

        let error = observe_wal(
            &path,
            &root.path().join("recorded-head.rec"),
            WAL_HARD_FRAME_LIMIT + 1,
        )
        .unwrap_err();

        assert!(error.contains("extent"));
    }

    #[test]
    fn wal_observation_rejects_corrupted_real_frame() {
        let root = tempfile::tempdir().unwrap();
        write_real_wal(root.path(), 1);
        let path = root.path().join("recorder.wal");
        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 0xff;
        fs::write(&path, bytes).unwrap();

        let error = observe_wal(&path, &root.path().join("recorded-head.rec"), 1).unwrap_err();

        assert!(error.contains("checksum"));
    }

    #[test]
    fn wal_observation_rejects_noncontiguous_real_frames() {
        let root = tempfile::tempdir().unwrap();
        write_real_wal(root.path(), 2);
        let path = root.path().join("recorder.wal");
        let mut bytes = fs::read(&path).unwrap();
        let first_len =
            usize::try_from(u64::from_be_bytes(bytes[6..14].try_into().unwrap())).unwrap();
        bytes[first_len + 22..first_len + 30].copy_from_slice(&3u64.to_be_bytes());
        let second_len = usize::try_from(u64::from_be_bytes(
            bytes[first_len + 6..first_len + 14].try_into().unwrap(),
        ))
        .unwrap();
        let digest_offset = first_len + second_len - 32;
        let digest = LogHash::digest(&[&bytes[first_len..digest_offset]]);
        bytes[digest_offset..first_len + second_len].copy_from_slice(digest.as_bytes());
        fs::write(&path, bytes).unwrap();

        let error = observe_wal(&path, &root.path().join("recorded-head.rec"), 2).unwrap_err();

        assert!(error.contains("sequence"));
    }

    #[test]
    fn persisted_state_rejects_checksum_valid_head_with_wrong_configuration_digest() {
        let root = tempfile::tempdir().unwrap();
        write_real_wal(root.path(), 1);
        let head_path = root.path().join("recorded-head.rec");
        let mut bytes = fs::read(&head_path).unwrap();
        let cluster_len = usize::from(u16::from_be_bytes(bytes[6..8].try_into().unwrap()));
        let config_digest_offset = 8 + cluster_len + 8 + 8;
        bytes[config_digest_offset] ^= 0xff;
        replace_trailing_digest(&mut bytes);
        fs::write(head_path, bytes).unwrap();

        let error = observe_reopenable_wal(root.path(), expected_membership(), 1).unwrap_err();

        assert!(error.contains("failed to reopen benchmark recorder"));
    }

    #[test]
    fn persisted_state_rejects_checksum_valid_head_with_malformed_tail() {
        let root = tempfile::tempdir().unwrap();
        write_real_wal(root.path(), 1);
        let head_path = root.path().join("recorded-head.rec");
        let mut bytes = fs::read(&head_path).unwrap();
        let digest_offset = bytes.len() - WAL_DIGEST_LEN;
        bytes.insert(digest_offset, 0xff);
        replace_trailing_digest(&mut bytes);
        fs::write(head_path, bytes).unwrap();

        let error = observe_reopenable_wal(root.path(), expected_membership(), 1).unwrap_err();

        assert!(error.contains("failed to reopen benchmark recorder"));
    }

    #[test]
    fn persisted_state_rejects_checksum_valid_structurally_truncated_wal_frame() {
        let root = tempfile::tempdir().unwrap();
        write_real_wal(root.path(), 1);
        let wal_path = root.path().join("recorder.wal");
        let mut bytes = fs::read(&wal_path).unwrap();
        let body_end = WAL_PREFIX_LEN + WAL_CHAIN_FIELDS_LEN + 8;
        bytes.truncate(body_end);
        let frame_len = body_end + WAL_DIGEST_LEN;
        bytes[6..14].copy_from_slice(&(frame_len as u64).to_be_bytes());
        let digest = LogHash::digest(&[&bytes]);
        bytes.extend_from_slice(digest.as_bytes());
        fs::write(wal_path, bytes).unwrap();

        let error = observe_reopenable_wal(root.path(), expected_membership(), 1).unwrap_err();

        assert!(error.contains("failed to reopen benchmark recorder"));
    }

    fn replace_trailing_digest(bytes: &mut [u8]) {
        let digest_offset = bytes.len() - WAL_DIGEST_LEN;
        let digest = LogHash::digest(&[&bytes[..digest_offset]]);
        bytes[digest_offset..].copy_from_slice(digest.as_bytes());
    }

    fn expected_membership() -> Membership {
        Membership::new(["n1", "n2", "n3"]).unwrap()
    }

    fn write_real_wal(root: &Path, operations: usize) {
        let membership = expected_membership();
        let config_digest = membership.digest();
        let store = RecorderFileStore::new_with_membership(
            root,
            RECORDER_ID,
            CLUSTER_ID,
            EPOCH,
            CONFIG_ID,
            membership,
        )
        .unwrap();
        let command = StoredCommand::new(EntryType::Command, b"payload".to_vec());
        for slot in 1..=operations {
            store
                .record_proposal(record_request(
                    config_digest,
                    slot as u64,
                    &command,
                    CommandMode::Inline,
                ))
                .unwrap();
        }

        drop(store);
    }
}
