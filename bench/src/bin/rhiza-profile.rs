use std::{
    collections::BTreeMap,
    env, fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rhiza_core::{
    Command as ConsensusCommand, CommandKind, ConfigurationState, EntryType, LogEntry, LogHash,
    Snapshot, SnapshotManifest,
};
use rhiza_graph::{encode_replicated_graph_command, GraphResultValue, LadybugStateMachine};
use rhiza_kv::{encode_replicated_kv_command, KvCommandV1, RedbStateMachine};
use rhiza_node::{NodeConfig, NodeRuntime, SqlWriteProfileSnapshot, SqlWriteProfiler};
use rhiza_quepaxa::{Membership, RecorderFileStore, RecorderRpc, RecorderRpcContext, ThreeNodeConsensus};
#[cfg(test)]
use rhiza_sql::decode_qwal_v3;
use rhiza_sql::{
    encode_sql_command, restore_snapshot_file, SqlBatchMember, SqlBatchPreparation,
    SqliteStateMachine, QWAL_V3_MAGIC,
};
use rhizadb::{
    effective_cluster_id, EmbeddedConfig, EmbeddedIdentity, ExecutionProfile, GraphCommandV1,
    GraphParameterValue, GraphValueV1, ReadConsistency, Rhiza, RhizaHandle, SqlCommand,
    SqlStatement, SqlValue,
};
use serde::{Deserialize, Serialize};

const KEYSPACE: u64 = 256;
const MAX_SQL_PADDING_MIB: usize = 1_024;
const SQL_PADDING_CHUNK_BYTES: usize = 128 * 1024;
const RAW_RESULT_BYTES: usize = 1024 * 1024;
const RAW_GRAPH_TIMEOUT_MS: u64 = 5_000;
const REPORT_SCHEMA_VERSION: u32 = 6;
const SQL_SEED_CACHE_MAGIC: &[u8] = b"RHIZA-SQL-SEED\0";
const SQL_SEED_CACHE_FORMAT_VERSION: u32 = 1;
const SQL_SEED_CACHE_HARNESS_VERSION: &str = "rhiza-profile-qwal-v3-seed-cache-v1";
const SQL_SEED_RECIPE_ID: &str = "sql-follower-padding-v1";
const SQL_SEED_ITEMS_DDL: &str =
    "CREATE TABLE bench_items(key TEXT PRIMARY KEY, value TEXT NOT NULL)";
const SQL_SEED_PADDING_DDL: &str =
    "CREATE TABLE bench_padding(id INTEGER PRIMARY KEY, payload BLOB NOT NULL)";
const SQL_SEED_PADDING_INSERT: &str = "INSERT INTO bench_padding(id, payload) VALUES (?1, ?2)";
const MAX_SQL_SEED_CACHE_HEADER_BYTES: usize = 64 * 1024;
const SQL_SEED_CACHE_FIXED_OVERHEAD_BYTES: u64 = 64 * 1024 * 1024;
type RecorderSet = Vec<(String, Box<dyn RecorderRpc>)>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Profile {
    Sql,
    Graph,
    Kv,
}

impl Profile {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "sql" => Ok(Self::Sql),
            "graph" => Ok(Self::Graph),
            "kv" => Ok(Self::Kv),
            _ => Err("--profile must be sql, graph, or kv".into()),
        }
    }

    const fn execution_profile(self) -> ExecutionProfile {
        match self {
            Self::Sql => ExecutionProfile::Sqlite,
            Self::Graph => ExecutionProfile::Graph,
            Self::Kv => ExecutionProfile::Kv,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Workload {
    Write,
    Get,
    DocumentGet,
    NativeRead,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum BenchmarkReadConsistency {
    Local,
    ReadBarrier,
}

impl BenchmarkReadConsistency {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "local" => Ok(Self::Local),
            "read_barrier" => Ok(Self::ReadBarrier),
            _ => Err("--consistency must be local or read_barrier".into()),
        }
    }

    const fn runtime(self) -> ReadConsistency {
        match self {
            Self::Local => ReadConsistency::Local,
            Self::ReadBarrier => ReadConsistency::ReadBarrier,
        }
    }

    const fn report(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::ReadBarrier => "read_barrier",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Layer {
    Handle,
    Runtime,
    Raw,
    Qwal,
    FollowerApply,
    Consensus,
}

impl Layer {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "handle" => Ok(Self::Handle),
            "runtime" => Ok(Self::Runtime),
            "raw" => Ok(Self::Raw),
            "qwal" => Ok(Self::Qwal),
            "follower-apply" => Ok(Self::FollowerApply),
            "consensus" => Ok(Self::Consensus),
            _ => Err(
                "--layer must be handle, runtime, raw, qwal, follower-apply, or consensus".into(),
            ),
        }
    }

    const fn scope(self) -> &'static str {
        match self {
            Self::Handle => "public RhizaHandle embedded API",
            Self::Runtime => {
                "NodeRuntime API with one blocking thread per benchmark worker; excludes RhizaHandle"
            }
            Self::Raw => {
                "direct materializer reads; writes encode a command, construct LogEntry, and apply it"
            }
            Self::Qwal => {
                "SQLite QWAL v3 batch encode, effect preparation, LogEntry construction, and materializer apply"
            }
            Self::FollowerApply => {
                "QWAL v3 preparation on an identically restored SQL leader plus timed SqliteStateMachine::apply_entry of a prebuilt LogEntry on a separate follower"
            }
            Self::Consensus => {
                "generic ThreeNodeConsensus::propose_at with deterministic payload; selected profile is not exercised; excludes qlog and materializer"
            }
        }
    }

    const fn consensus(self) -> &'static str {
        match self {
            Self::Handle | Self::Runtime => {
                "in-process QuePaxa with three file-backed RecorderRpc voters"
            }
            Self::Raw | Self::Qwal | Self::FollowerApply => {
                "excluded; writes include command encode, LogEntry construction, and materializer apply"
            }
            Self::Consensus => "in-process QuePaxa with three file-backed RecorderRpc voters",
        }
    }

    const fn durability(self, profile: Profile) -> &'static str {
        match self {
            Self::Handle | Self::Runtime if matches!(profile, Profile::Kv) => {
                "Recorder quorum durability plus one atomic redb commit containing the KV state and full qlog entry; file qlog is a buffered rehydratable mirror"
            }
            Self::Handle | Self::Runtime => {
                "Recorder quorum is the authoritative durable redo log; SQLite, control, and file qlog are non-durable rebuildable local views"
            }
            Self::Raw => "materializer-native local commit only",
            Self::Qwal => {
                "non-durable local QWAL batch preparation and SQLite materializer apply; excludes consensus and qlog"
            }
            Self::FollowerApply => {
                "non-durable follower SQLite materializer apply from a leader-prepared QWAL; excludes consensus and qlog"
            }
            Self::Consensus => {
                "three RecorderFileStore voter commits; excludes local qlog and materializer"
            }
        }
    }
}

impl Workload {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "write" => Ok(Self::Write),
            "get" => Ok(Self::Get),
            "document-get" => Ok(Self::DocumentGet),
            "native-read" => Ok(Self::NativeRead),
            _ => Err("--workload must be write, get, document-get, or native-read".into()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    layer: Layer,
    profile: Profile,
    workload: Workload,
    batch_size: usize,
    operations: u64,
    warmup: u64,
    concurrency: usize,
    value_bytes: usize,
    read_consistency: BenchmarkReadConsistency,
    sql_write_profile: bool,
    sql_padding_mib: usize,
    sql_seed_cache: Option<PathBuf>,
}

impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let values: Vec<_> = args.into_iter().collect();
        let mut profile = None;
        let mut workload = None;
        let mut layer = Layer::Handle;
        let mut batch_size = 1;
        let mut operations = 1_000;
        let mut warmup = 100;
        let mut concurrency = 1;
        let mut value_bytes = 128;
        let mut read_consistency = BenchmarkReadConsistency::Local;
        let mut consistency_was_explicit = false;
        let mut sql_write_profile = false;
        let mut sql_padding_mib = 0;
        let mut sql_padding_was_explicit = false;
        let mut sql_seed_cache = None;
        let mut index = 0;
        while index < values.len() {
            let flag = &values[index];
            if flag == "--sql-write-profile" {
                sql_write_profile = true;
                index += 1;
                continue;
            }
            let next = || {
                values
                    .get(index + 1)
                    .ok_or_else(|| format!("{flag} requires a value"))
            };
            match flag.as_str() {
                "--layer" => layer = Layer::parse(next()?)?,
                "--profile" => profile = Some(Profile::parse(next()?)?),
                "--workload" => workload = Some(Workload::parse(next()?)?),
                "--batch-size" => batch_size = parse_usize(next()?, flag)?,
                "--operations" => operations = parse_u64(next()?, flag)?,
                "--warmup" => warmup = parse_u64_allow_zero(next()?, flag)?,
                "--concurrency" => concurrency = parse_usize(next()?, flag)?,
                "--value-bytes" => value_bytes = parse_usize(next()?, flag)?,
                "--sql-padding-mib" => {
                    sql_padding_mib = parse_usize_allow_zero(next()?, flag)?;
                    sql_padding_was_explicit = true;
                }
                "--sql-seed-cache" => {
                    let path = PathBuf::from(next()?);
                    if path.as_os_str().is_empty() {
                        return Err("--sql-seed-cache requires a non-empty file path".into());
                    }
                    sql_seed_cache = Some(path);
                }
                "--consistency" => {
                    read_consistency = BenchmarkReadConsistency::parse(next()?)?;
                    consistency_was_explicit = true;
                }
                "--help" | "-h" => return Err(usage()),
                _ => return Err(format!("unknown option: {flag}\n\n{}", usage())),
            }
            index += 2;
        }
        if !(16..=4_096).contains(&value_bytes) {
            return Err("--value-bytes must be between 16 and 4096".into());
        }
        if sql_padding_mib > MAX_SQL_PADDING_MIB {
            return Err(format!(
                "--sql-padding-mib must be between 0 and {MAX_SQL_PADDING_MIB}"
            ));
        }
        if matches!(
            layer,
            Layer::Raw | Layer::Qwal | Layer::FollowerApply | Layer::Consensus
        ) && concurrency != 1
        {
            return Err(format!("--layer {layer:?} requires --concurrency 1").to_lowercase());
        }
        let profile = profile.ok_or_else(|| "--profile is required".to_string())?;
        let workload = workload.ok_or_else(|| "--workload is required".to_string())?;
        if workload == Workload::DocumentGet && profile != Profile::Graph {
            return Err("--workload document-get requires --profile graph".into());
        }
        if layer == Layer::Consensus && workload != Workload::Write {
            return Err("--layer consensus supports only --workload write".into());
        }
        if layer == Layer::Raw && profile == Profile::Sql && workload == Workload::Write {
            return Err(
                "--layer raw cannot measure SQL writes: SQLite apply accepts only prepared QWAL v3 effects; use --layer qwal"
                    .into(),
            );
        }
        if matches!(layer, Layer::Qwal | Layer::FollowerApply)
            && (profile != Profile::Sql || workload != Workload::Write)
        {
            return Err(format!(
                "--layer {} supports only --profile sql --workload write",
                match layer {
                    Layer::Qwal => "qwal",
                    Layer::FollowerApply => "follower-apply",
                    _ => unreachable!(),
                }
            ));
        }
        if sql_padding_was_explicit && layer != Layer::FollowerApply {
            return Err("--sql-padding-mib requires --layer follower-apply".into());
        }
        if sql_seed_cache.is_some() && layer != Layer::FollowerApply {
            return Err("--sql-seed-cache requires --layer follower-apply".into());
        }
        if consistency_was_explicit
            && (workload == Workload::Write || !matches!(layer, Layer::Handle | Layer::Runtime))
        {
            return Err(
                "--consistency is supported only for handle or runtime read workloads".into(),
            );
        }
        if sql_write_profile
            && !(layer == Layer::Runtime && profile == Profile::Sql && workload == Workload::Write)
        {
            return Err(
                "--sql-write-profile requires --layer runtime --profile sql --workload write"
                    .into(),
            );
        }
        if !matches!(
            batch_size,
            1 | 2 | 4 | 8 | 16 | 32 | 64 | 128 | 256 | 512 | 1024
        ) {
            return Err(
                "--batch-size must be 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, or 1024".into(),
            );
        }
        if batch_size > 256 && !matches!(layer, Layer::Qwal | Layer::FollowerApply) {
            return Err("--batch-size 512 or 1024 requires --layer qwal or follower-apply".into());
        }
        if batch_size != 1 && workload != Workload::Write {
            return Err("--batch-size greater than 1 requires --workload write".into());
        }
        if batch_size != 1
            && !matches!(
                layer,
                Layer::Handle | Layer::Runtime | Layer::Qwal | Layer::FollowerApply
            )
        {
            return Err(
                "--batch-size greater than 1 requires --layer handle, runtime, qwal, or follower-apply"
                    .into(),
            );
        }
        if batch_size > 64
            && profile == Profile::Graph
            && matches!(layer, Layer::Handle | Layer::Runtime)
        {
            let layer = match layer {
                Layer::Handle => "handle",
                Layer::Runtime => "runtime",
                Layer::Raw | Layer::Qwal | Layer::FollowerApply | Layer::Consensus => {
                    unreachable!("only embedded typed batch layers reach this check")
                }
            };
            return Err(format!(
                "--profile graph supports --batch-size at most 64 on --layer {layer}"
            ));
        }
        Ok(Self {
            layer,
            profile,
            workload,
            batch_size,
            operations,
            warmup,
            concurrency,
            value_bytes,
            read_consistency,
            sql_write_profile,
            sql_padding_mib,
            sql_seed_cache,
        })
    }
}

fn parse_u64(value: &str, flag: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be a positive integer"))?;
    if parsed == 0 {
        Err(format!("{flag} must be a positive integer"))
    } else {
        Ok(parsed)
    }
}

fn parse_u64_allow_zero(value: &str, flag: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{flag} must be a non-negative integer"))
}

fn parse_usize(value: &str, flag: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{flag} must be a positive integer"))?;
    if parsed == 0 {
        Err(format!("{flag} must be a positive integer"))
    } else {
        Ok(parsed)
    }
}

fn parse_usize_allow_zero(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .map_err(|_| format!("{flag} must be a non-negative integer"))
}

fn usage() -> String {
    "usage: rhiza-profile --profile sql|graph|kv --workload write|get|document-get|native-read \
     [--layer handle|runtime|raw|qwal|follower-apply|consensus] [--operations N] [--warmup N] [--concurrency N] \
     [--batch-size 1|2|4|8|16|32|64|128|256|512|1024] [--value-bytes N] [--consistency local|read_barrier] \
     [--sql-write-profile] [--sql-padding-mib 0..1024] [--sql-seed-cache FILE]"
        .into()
}

#[derive(Clone, Debug, Default)]
struct Samples {
    successes: u64,
    errors: u64,
    latency_us: BTreeMap<u64, u64>,
    error_classes: BTreeMap<String, u64>,
    batch_calls: u64,
    successful_batch_calls: u64,
    failed_batch_calls: u64,
    batch_call_latency_us: BTreeMap<u64, u64>,
    qwal_prepare_calls: u64,
    qwal_prepare_latency_us: BTreeMap<u64, u64>,
    qwal_apply_calls: u64,
    qwal_apply_latency_us: BTreeMap<u64, u64>,
    follower_apply_calls: u64,
    follower_apply_latency_us: BTreeMap<u64, u64>,
    qwal_envelope_bytes: ByteSamples,
    sql_write_profile: SqlWritePhaseSamples,
}

impl Samples {
    fn record(&mut self, latency: Duration, result: Result<(), String>) {
        let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
        match result {
            Ok(()) => {
                self.successes += 1;
                *self.latency_us.entry(micros).or_default() += 1;
            }
            Err(error) => {
                self.errors += 1;
                let mut class = error.replace(['\n', '\r'], " ");
                class.truncate(160);
                if self.error_classes.len() < 16 || self.error_classes.contains_key(&class) {
                    *self.error_classes.entry(class).or_default() += 1;
                } else {
                    *self.error_classes.entry("other".into()).or_default() += 1;
                }
            }
        }
    }

    fn record_batch(&mut self, latency: Duration, results: Vec<Result<(), String>>) {
        let count = results.len().max(1) as u32;
        let logical_item_latency = latency / count;
        let succeeded = results.iter().all(Result::is_ok);
        self.batch_calls += 1;
        if succeeded {
            self.successful_batch_calls += 1;
            let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
            *self.batch_call_latency_us.entry(micros).or_default() += 1;
        } else {
            self.failed_batch_calls += 1;
        }
        for result in results {
            self.record(logical_item_latency, result);
        }
    }

    fn record_qwal_envelope(&mut self, bytes: usize) {
        self.qwal_envelope_bytes.record(bytes);
    }

    fn record_qwal_phases(&mut self, prepare: Duration, apply: Option<Duration>) {
        self.qwal_prepare_calls += 1;
        record_latency(&mut self.qwal_prepare_latency_us, prepare);
        if let Some(apply) = apply {
            self.qwal_apply_calls += 1;
            record_latency(&mut self.qwal_apply_latency_us, apply);
        }
    }

    fn record_follower_apply(&mut self, apply: Duration) {
        self.follower_apply_calls += 1;
        record_latency(&mut self.follower_apply_latency_us, apply);
    }

    fn merge(&mut self, other: Self) {
        self.successes += other.successes;
        self.errors += other.errors;
        for (latency, count) in other.latency_us {
            *self.latency_us.entry(latency).or_default() += count;
        }
        for (class, count) in other.error_classes {
            *self.error_classes.entry(class).or_default() += count;
        }
        self.batch_calls += other.batch_calls;
        self.successful_batch_calls += other.successful_batch_calls;
        self.failed_batch_calls += other.failed_batch_calls;
        for (latency, count) in other.batch_call_latency_us {
            *self.batch_call_latency_us.entry(latency).or_default() += count;
        }
        self.qwal_prepare_calls += other.qwal_prepare_calls;
        for (latency, count) in other.qwal_prepare_latency_us {
            *self.qwal_prepare_latency_us.entry(latency).or_default() += count;
        }
        self.qwal_apply_calls += other.qwal_apply_calls;
        for (latency, count) in other.qwal_apply_latency_us {
            *self.qwal_apply_latency_us.entry(latency).or_default() += count;
        }
        self.follower_apply_calls += other.follower_apply_calls;
        for (latency, count) in other.follower_apply_latency_us {
            *self.follower_apply_latency_us.entry(latency).or_default() += count;
        }
        self.qwal_envelope_bytes.merge(other.qwal_envelope_bytes);
        self.sql_write_profile.merge(other.sql_write_profile);
    }

    fn percentile(&self, permille: u64) -> Option<u64> {
        if self.successes == 0 {
            return None;
        }
        let rank = self
            .successes
            .saturating_mul(permille)
            .div_ceil(1_000)
            .max(1);
        let mut seen = 0;
        self.latency_us.iter().find_map(|(latency, count)| {
            seen += count;
            (seen >= rank).then_some(*latency)
        })
    }

    fn metrics(&self, elapsed: Duration, qlog_entries: Option<u64>) -> Metrics {
        let batch_call_latency_us =
            latencies(&self.batch_call_latency_us, self.successful_batch_calls);
        Metrics {
            attempts: self.successes + self.errors,
            successes: self.successes,
            errors: self.errors,
            elapsed_seconds: elapsed.as_secs_f64(),
            operations_per_second: if elapsed.is_zero() {
                0.0
            } else {
                self.successes as f64 / elapsed.as_secs_f64()
            },
            qlog_entries,
            logical_operations_per_qlog: qlog_entries
                .filter(|entries| *entries > 0)
                .map(|entries| self.successes as f64 / entries as f64),
            batch_calls: (self.batch_calls > 0).then_some(self.batch_calls),
            successful_batch_calls: (self.batch_calls > 0).then_some(self.successful_batch_calls),
            failed_batch_calls: (self.batch_calls > 0).then_some(self.failed_batch_calls),
            batch_calls_per_second: (self.batch_calls > 0).then(|| {
                if elapsed.is_zero() {
                    0.0
                } else {
                    self.batch_calls as f64 / elapsed.as_secs_f64()
                }
            }),
            batch_call_latency_us,
            qwal_prepare_latency_us: latencies(
                &self.qwal_prepare_latency_us,
                self.qwal_prepare_calls,
            ),
            qwal_apply_latency_us: latencies(&self.qwal_apply_latency_us, self.qwal_apply_calls),
            follower_apply_latency_us: latencies(
                &self.follower_apply_latency_us,
                self.follower_apply_calls,
            ),
            qwal_envelope_bytes: self.qwal_envelope_bytes.metrics(),
            sql_write_profile: self.sql_write_profile.metrics(),
            logical_item_latency_us: Latencies {
                p50: self.percentile(500),
                p95: self.percentile(950),
                p99: self.percentile(990),
                p99_9: self.percentile(999),
                max: self.latency_us.last_key_value().map(|(value, _)| *value),
            },
            error_classes: self.error_classes.clone(),
        }
    }
}

fn record_latency(samples: &mut BTreeMap<u64, u64>, latency: Duration) {
    let micros = u64::try_from(latency.as_micros()).unwrap_or(u64::MAX);
    *samples.entry(micros).or_default() += 1;
}

fn latencies(samples: &BTreeMap<u64, u64>, count: u64) -> Option<Latencies> {
    (count > 0).then(|| Latencies {
        p50: percentile(samples, count, 500),
        p95: percentile(samples, count, 950),
        p99: percentile(samples, count, 990),
        p99_9: percentile(samples, count, 999),
        max: samples.last_key_value().map(|(value, _)| *value),
    })
}

fn percentile(samples: &BTreeMap<u64, u64>, count: u64, permille: u64) -> Option<u64> {
    if count == 0 {
        return None;
    }
    let rank = count.saturating_mul(permille).div_ceil(1_000).max(1);
    let mut seen = 0;
    samples.iter().find_map(|(latency, bucket_count)| {
        seen += bucket_count;
        (seen >= rank).then_some(*latency)
    })
}

#[derive(Clone, Debug, Default)]
struct ByteSamples {
    count: u64,
    total: u64,
    min: Option<u64>,
    max: Option<u64>,
}

impl ByteSamples {
    fn record(&mut self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.count += 1;
        self.total = self.total.saturating_add(bytes);
        self.min = Some(self.min.map_or(bytes, |current| current.min(bytes)));
        self.max = Some(self.max.map_or(bytes, |current| current.max(bytes)));
    }

    fn merge(&mut self, other: Self) {
        if other.count == 0 {
            return;
        }
        self.count += other.count;
        self.total = self.total.saturating_add(other.total);
        if let Some(min) = other.min {
            self.min = Some(self.min.map_or(min, |current| current.min(min)));
        }
        if let Some(max) = other.max {
            self.max = Some(self.max.map_or(max, |current| current.max(max)));
        }
    }

    fn metrics(&self) -> Option<QwalEnvelopeBytes> {
        (self.count > 0).then(|| QwalEnvelopeBytes {
            count: self.count,
            total: self.total,
            average: self.total as f64 / self.count as f64,
            min: self.min.expect("non-empty byte samples have a minimum"),
            max: self.max.expect("non-empty byte samples have a maximum"),
        })
    }
}

#[derive(Clone, Debug, Default)]
struct PhaseSamples {
    count: u64,
    total_us: u64,
    latency_us: BTreeMap<u64, u64>,
}

impl PhaseSamples {
    fn record(&mut self, micros: u64) {
        self.count += 1;
        self.total_us = self.total_us.saturating_add(micros);
        *self.latency_us.entry(micros).or_default() += 1;
    }

    fn merge(&mut self, other: Self) {
        self.count += other.count;
        self.total_us = self.total_us.saturating_add(other.total_us);
        for (latency, count) in other.latency_us {
            *self.latency_us.entry(latency).or_default() += count;
        }
    }

    fn metrics(&self) -> PhaseMetrics {
        PhaseMetrics {
            total_us: self.total_us,
            latency_us: latencies(&self.latency_us, self.count)
                .expect("a profiled phase has at least one sample"),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct SqlWritePhaseSamples {
    sample_count: u64,
    member_count: u64,
    commit_lock_wait: PhaseSamples,
    precheck_classification: PhaseSamples,
    qwal_prepare: PhaseSamples,
    consensus_propose: PhaseSamples,
    local_qlog_mirror_append: PhaseSamples,
    sql_materializer_apply: PhaseSamples,
    response_other_total: PhaseSamples,
    total_service: PhaseSamples,
}

impl SqlWritePhaseSamples {
    fn record_snapshot(
        &mut self,
        snapshot: SqlWriteProfileSnapshot,
        expected_samples: u64,
        expected_members: u64,
    ) -> Result<(), String> {
        if snapshot.dropped_samples != 0 {
            return Err(format!(
                "SQL write profiler dropped {} samples",
                snapshot.dropped_samples
            ));
        }
        let sample_count = u64::try_from(snapshot.samples.len()).unwrap_or(u64::MAX);
        if sample_count != expected_samples {
            return Err(format!(
                "SQL write profiler returned {sample_count} samples; expected {expected_samples}"
            ));
        }
        for sample in snapshot.samples {
            let named_total = sample
                .commit_lock_wait_us
                .saturating_add(sample.precheck_classification_us)
                .saturating_add(sample.qwal_prepare_us)
                .saturating_add(sample.consensus_propose_us)
                .saturating_add(sample.local_qlog_mirror_append_us)
                .saturating_add(sample.sql_materializer_apply_us)
                .saturating_add(sample.response_other_total_us);
            if named_total != sample.total_service_us {
                return Err(format!(
                    "SQL write profiler phase sum {named_total} differs from total service {}",
                    sample.total_service_us
                ));
            }
            self.sample_count += 1;
            self.member_count = self
                .member_count
                .saturating_add(u64::try_from(sample.batch_member_count).unwrap_or(u64::MAX));
            self.commit_lock_wait.record(sample.commit_lock_wait_us);
            self.precheck_classification
                .record(sample.precheck_classification_us);
            self.qwal_prepare.record(sample.qwal_prepare_us);
            self.consensus_propose.record(sample.consensus_propose_us);
            self.local_qlog_mirror_append
                .record(sample.local_qlog_mirror_append_us);
            self.sql_materializer_apply
                .record(sample.sql_materializer_apply_us);
            self.response_other_total
                .record(sample.response_other_total_us);
            self.total_service.record(sample.total_service_us);
        }
        if self.member_count != expected_members {
            return Err(format!(
                "SQL write profiler covered {} members; expected {expected_members}",
                self.member_count
            ));
        }
        Ok(())
    }

    fn merge(&mut self, other: Self) {
        self.sample_count += other.sample_count;
        self.member_count += other.member_count;
        self.commit_lock_wait.merge(other.commit_lock_wait);
        self.precheck_classification
            .merge(other.precheck_classification);
        self.qwal_prepare.merge(other.qwal_prepare);
        self.consensus_propose.merge(other.consensus_propose);
        self.local_qlog_mirror_append
            .merge(other.local_qlog_mirror_append);
        self.sql_materializer_apply
            .merge(other.sql_materializer_apply);
        self.response_other_total.merge(other.response_other_total);
        self.total_service.merge(other.total_service);
    }

    fn metrics(&self) -> Option<SqlWriteProfileMetrics> {
        (self.sample_count > 0).then(|| SqlWriteProfileMetrics {
            sample_count: self.sample_count,
            member_count: self.member_count,
            dropped_samples: 0,
            phase_latency_us: SqlWritePhaseMetrics {
                commit_lock_wait: self.commit_lock_wait.metrics(),
                precheck_classification: self.precheck_classification.metrics(),
                qwal_prepare: self.qwal_prepare.metrics(),
                consensus_propose: self.consensus_propose.metrics(),
                local_qlog_mirror_append: self.local_qlog_mirror_append.metrics(),
                sql_materializer_apply: self.sql_materializer_apply.metrics(),
                response_other_total: self.response_other_total.metrics(),
                total_service: self.total_service.metrics(),
            },
        })
    }
}

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u32,
    benchmark: &'static str,
    generated_at_unix_ms: u128,
    provenance: Provenance,
    system: System,
    configuration: ReportConfig,
    measurement: Metrics,
    limitations: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct Provenance {
    git_commit: String,
    git_dirty: bool,
    rustc: String,
    source: &'static str,
}

#[derive(Debug, Serialize)]
struct System {
    os: &'static str,
    architecture: &'static str,
    logical_cpus: usize,
    kernel: String,
    cpu_model: String,
}

#[derive(Debug, Serialize)]
struct ReportConfig {
    layer: Layer,
    measurement_scope: &'static str,
    profile: Profile,
    workload: Workload,
    batch_size: usize,
    logical_operations: u64,
    operations: u64,
    warmup_operations: u64,
    concurrency: usize,
    keyspace: u64,
    value_bytes: usize,
    sql_padding_mib: usize,
    read_consistency: &'static str,
    consensus: &'static str,
    durability: &'static str,
    sql_write_profile: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    follower_apply_latency_scope: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    follower_seed_state: Option<FollowerSeedState>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct FollowerSeedState {
    receipt_count: u64,
    leader_database_bytes: u64,
    follower_database_bytes: u64,
    leader_control_bytes: u64,
    follower_control_bytes: u64,
    leader_embedded_qlog_entries: u64,
    follower_embedded_qlog_entries: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    seed_cache: Option<SqlSeedCacheProvenance>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct SqlSeedCacheProvenance {
    disposition: &'static str,
    path: String,
    digest: String,
    bytes: u64,
    snapshot_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SqlSeedCacheHeader {
    format_version: u32,
    harness_version: String,
    report_schema_version: u32,
    seed_recipe_id: String,
    seed_recipe_digest: String,
    profile: String,
    workload: String,
    padding_mib: usize,
    value_bytes: usize,
    keyspace: u64,
    receipt_count: u64,
    snapshot_bytes: u64,
    snapshot_digest: String,
    configuration_state: ConfigurationState,
    snapshot_manifest: SnapshotManifest,
}

struct LoadedSqlSeedCache {
    snapshot: Snapshot,
    provenance: SqlSeedCacheProvenance,
}

#[derive(Clone, Debug, Serialize)]
struct Metrics {
    attempts: u64,
    successes: u64,
    errors: u64,
    elapsed_seconds: f64,
    operations_per_second: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    qlog_entries: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    logical_operations_per_qlog: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_calls: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    successful_batch_calls: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failed_batch_calls: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_calls_per_second: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    batch_call_latency_us: Option<Latencies>,
    #[serde(skip_serializing_if = "Option::is_none")]
    qwal_prepare_latency_us: Option<Latencies>,
    #[serde(skip_serializing_if = "Option::is_none")]
    qwal_apply_latency_us: Option<Latencies>,
    #[serde(skip_serializing_if = "Option::is_none")]
    follower_apply_latency_us: Option<Latencies>,
    #[serde(skip_serializing_if = "Option::is_none")]
    qwal_envelope_bytes: Option<QwalEnvelopeBytes>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sql_write_profile: Option<SqlWriteProfileMetrics>,
    /// Amortized batch service time per logical item for write workloads, and
    /// end-to-end call latency for non-batched workloads.
    logical_item_latency_us: Latencies,
    error_classes: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct QwalEnvelopeBytes {
    count: u64,
    total: u64,
    average: f64,
    min: u64,
    max: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct SqlWriteProfileMetrics {
    sample_count: u64,
    member_count: u64,
    dropped_samples: u64,
    phase_latency_us: SqlWritePhaseMetrics,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct SqlWritePhaseMetrics {
    commit_lock_wait: PhaseMetrics,
    precheck_classification: PhaseMetrics,
    qwal_prepare: PhaseMetrics,
    consensus_propose: PhaseMetrics,
    local_qlog_mirror_append: PhaseMetrics,
    sql_materializer_apply: PhaseMetrics,
    response_other_total: PhaseMetrics,
    total_service: PhaseMetrics,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct PhaseMetrics {
    total_us: u64,
    latency_us: Latencies,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct Latencies {
    p50: Option<u64>,
    p95: Option<u64>,
    p99: Option<u64>,
    #[serde(rename = "p99.9")]
    p99_9: Option<u64>,
    max: Option<u64>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let config = match Config::parse(env::args().skip(1)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    match run(config).await {
        Ok(report) => {
            let failed = report.measurement.errors > 0;
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
            if failed {
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("rhiza-profile: {error}");
            std::process::exit(1);
        }
    }
}

async fn run(config: Config) -> Result<Report, String> {
    // Capture before setup or measurement so benchmark-created files cannot
    // change the source provenance reported for this run.
    let provenance = provenance();
    let root = tempfile::tempdir().map_err(|error| error.to_string())?;
    let (samples, elapsed, qlog_entries, follower_seed_state) = match config.layer {
        Layer::Handle => {
            let rhiza = Rhiza::open(embedded_config(root.path(), config.profile)?)
                .await
                .map_err(|error| error.to_string())?;
            let measured = measure_target(Target::Handle(rhiza.handle()), &config, None).await;
            let shutdown = rhiza.shutdown().await.map_err(|error| error.to_string());
            let measured = measured?;
            shutdown?;
            let (samples, elapsed, qlog_entries) = measured;
            (samples, elapsed, qlog_entries, None)
        }
        Layer::Runtime => {
            let (runtime, profiler) = runtime(root.path(), &config)?;
            let (samples, elapsed, qlog_entries) =
                measure_target(Target::Runtime(runtime), &config, profiler).await?;
            (samples, elapsed, qlog_entries, None)
        }
        Layer::Raw | Layer::Qwal => {
            let (samples, elapsed) =
                measure_raw(RawTarget::open(root.path(), config.profile)?, &config)?;
            (samples, elapsed, None, None)
        }
        Layer::FollowerApply => {
            let (samples, elapsed, seed_state) = measure_follower(root.path(), &config)?;
            (samples, elapsed, None, Some(seed_state))
        }
        Layer::Consensus => {
            let (samples, elapsed) =
                measure_consensus(ConsensusTarget::open(root.path())?, &config)?;
            (samples, elapsed, None, None)
        }
    };

    let mut limitations = vec![
        "single process on one host",
        "excludes HTTP serialization and transport",
        "excludes node-to-node network latency",
        "excludes remote checkpoint upload",
    ];
    if config.workload != Workload::Write
        && config.read_consistency == BenchmarkReadConsistency::Local
    {
        limitations.push("local reads exclude a consensus read barrier");
    }
    if config.layer == Layer::Qwal {
        limitations.push(
            "QWAL latency combines effect preparation and apply; internal phase timings are not exposed by this report",
        );
    }
    if config.layer == Layer::FollowerApply {
        limitations.push(
            "follower_apply_latency_us measures only SqliteStateMachine::apply_entry for a prebuilt follower LogEntry; payload clone/hash/entry construction, anchor bookkeeping, leader QWAL preparation, and leader catch-up are excluded",
        );
        limitations.push(
            "SQL padding is seeded before a recovery-snapshot restore into fresh leader/follower views; seed receipts remain in control and are reported, while embedded qlog is cleared",
        );
        if config.sql_seed_cache.is_some() {
            limitations.push(
                "the explicit SQL seed cache affects setup time only: a cache hit skips QWAL seeding, while both cache hits and misses restore and fully validate fresh leader/follower materializers before warmup",
            );
        }
    }
    if config.workload == Workload::Write {
        limitations.push(
            "logical_item_latency_us is batch-call elapsed divided by submitted item count; batch_call_latency_us is end-to-end physical call latency",
        );
    }
    if config.sql_write_profile {
        limitations.push(
            "sql_write_profile total_service includes commit_lock_wait; subtract that phase for on-lock service time",
        );
    }

    Ok(Report {
        schema_version: REPORT_SCHEMA_VERSION,
        benchmark: "rhiza-profile-direct",
        generated_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
        provenance,
        system: system(),
        configuration: ReportConfig {
            layer: config.layer,
            measurement_scope: config.layer.scope(),
            profile: config.profile,
            workload: config.workload,
            batch_size: config.batch_size,
            logical_operations: config.operations,
            operations: config.operations,
            warmup_operations: config.warmup,
            concurrency: config.concurrency,
            keyspace: KEYSPACE,
            value_bytes: config.value_bytes,
            sql_padding_mib: config.sql_padding_mib,
            read_consistency: config.read_consistency.report(),
            consensus: config.layer.consensus(),
            durability: config.layer.durability(config.profile),
            sql_write_profile: config.sql_write_profile,
            follower_apply_latency_scope: (config.layer == Layer::FollowerApply).then_some(
                "SqliteStateMachine::apply_entry on a prebuilt leader LogEntry only; payload clone/hash/entry construction and anchor bookkeeping are excluded",
            ),
            follower_seed_state,
        },
        measurement: samples.metrics(elapsed, qlog_entries),
        limitations,
    })
}

fn embedded_config(root: &std::path::Path, profile: Profile) -> Result<EmbeddedConfig, String> {
    let execution_profile = profile.execution_profile();
    let membership =
        Membership::new(["node-1", "node-2", "node-3"]).map_err(|error| error.to_string())?;
    let cluster_id = effective_cluster_id(execution_profile, "profile-bench")
        .map_err(|error| error.to_string())?;
    let recorders = recorders(root, &membership, &cluster_id)?;
    Ok(EmbeddedConfig::new(
        EmbeddedIdentity::new("profile-bench", "node-1", 1, 1),
        root.join("node"),
        execution_profile,
        membership.members().to_vec(),
        recorders,
        vec![],
        None,
    ))
}

fn recorders(
    root: &Path,
    membership: &Membership,
    cluster_id: &str,
) -> Result<RecorderSet, String> {
    membership
        .members()
        .iter()
        .map(|id| {
            RecorderFileStore::new_with_membership(
                root.join("recorders").join(id),
                id.clone(),
                cluster_id,
                1,
                1,
                membership.clone(),
            )
            .map(|recorder| (id.clone(), Box::new(recorder) as Box<dyn RecorderRpc>))
            .map_err(|error| error.to_string())
        })
        .collect()
}

fn runtime(
    root: &Path,
    config: &Config,
) -> Result<(Arc<NodeRuntime>, Option<SqlWriteProfiler>), String> {
    let execution_profile = config.profile.execution_profile();
    let membership =
        Membership::new(["node-1", "node-2", "node-3"]).map_err(|error| error.to_string())?;
    let mut node_config = NodeConfig::new_embedded(
        "profile-bench",
        "node-1",
        root.join("node"),
        1,
        1,
        membership.members().to_vec(),
    )
    .map_err(|error| error.to_string())?
    .with_execution_profile(execution_profile)
    .map_err(|error| error.to_string())?;
    let profiler = config.sql_write_profile.then(|| {
        let capacity = config
            .operations
            .saturating_add(config.warmup)
            .saturating_add(KEYSPACE)
            .saturating_add(1);
        SqlWriteProfiler::new(usize::try_from(capacity).unwrap_or(usize::MAX))
    });
    if let Some(profiler) = profiler.clone() {
        node_config = node_config.with_sql_write_profiler(profiler);
    }
    let consensus = Arc::new(
        ThreeNodeConsensus::from_recorders_with_ids(
            node_config.cluster_id().to_owned(),
            "node-1",
            1,
            1,
            recorders(root, &membership, node_config.cluster_id())?,
        )
        .map_err(|error| error.to_string())?,
    );
    if node_config.membership() != consensus.membership() {
        return Err("runtime benchmark membership mismatch".into());
    }
    NodeRuntime::open(node_config, consensus, &[])
        .map(Arc::new)
        .map(|runtime| (runtime, profiler))
        .map_err(|error| error.to_string())
}

#[derive(Clone)]
enum Target {
    Handle(RhizaHandle),
    Runtime(Arc<NodeRuntime>),
}

async fn measure_target(
    target: Target,
    config: &Config,
    profiler: Option<SqlWriteProfiler>,
) -> Result<(Samples, Duration, Option<u64>), String> {
    setup(&target, config).await?;
    if config.warmup > 0 {
        let (warmup, _) = run_phase(target.clone(), config, config.warmup, "warmup").await;
        if warmup.errors > 0 {
            return Err(format!("warmup failed with {} errors", warmup.errors));
        }
    }
    if let Some(profiler) = &profiler {
        profiler.drain();
    }
    let qlog_before = match &target {
        Target::Handle(_) => None,
        Target::Runtime(runtime) => {
            Some(runtime.applied_index().map_err(|error| error.to_string())?)
        }
    };
    let measured = run_phase(target.clone(), config, config.operations, "measure").await;
    let qlog_entries = match (&target, qlog_before) {
        (Target::Runtime(runtime), Some(before)) => Some(
            runtime
                .applied_index()
                .map_err(|error| error.to_string())?
                .saturating_sub(before),
        ),
        _ => None,
    };
    let (mut samples, elapsed) = measured;
    if let Some(profiler) = profiler {
        samples.sql_write_profile.record_snapshot(
            profiler.drain(),
            qlog_entries.unwrap_or(samples.batch_calls),
            samples.successes,
        )?;
    }
    Ok((samples, elapsed, qlog_entries))
}

async fn setup(target: &Target, config: &Config) -> Result<(), String> {
    if config.profile == Profile::Sql {
        execute_sql(
            target,
            SqlCommand {
                request_id: "profile-bench-schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE bench_items(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                        .into(),
                    parameters: vec![],
                }],
            },
        )
        .await?;
    }
    for index in 0..KEYSPACE {
        write_one(
            target,
            config.profile,
            index,
            &format!("setup-{index:016x}"),
            config.value_bytes,
        )
        .await?;
    }
    Ok(())
}

async fn run_phase(
    target: Target,
    config: &Config,
    operations: u64,
    phase: &'static str,
) -> (Samples, Duration) {
    match target {
        Target::Handle(handle) => run_phase_handle(handle, config, operations, phase).await,
        Target::Runtime(runtime) => run_phase_runtime(runtime, config, operations, phase).await,
    }
}

async fn run_phase_handle(
    handle: RhizaHandle,
    config: &Config,
    operations: u64,
    phase: &'static str,
) -> (Samples, Duration) {
    let counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now() + Duration::from_millis(20);
    let mut workers = Vec::with_capacity(config.concurrency);
    for _ in 0..config.concurrency {
        let target = Target::Handle(handle.clone());
        let counter = counter.clone();
        let config = config.clone();
        workers.push(tokio::spawn(async move {
            tokio::time::sleep_until(start.into()).await;
            let mut samples = Samples::default();
            loop {
                let claim = if config.workload == Workload::Write {
                    config.batch_size as u64
                } else {
                    1
                };
                let sequence = counter.fetch_add(claim, Ordering::Relaxed);
                if sequence >= operations {
                    break;
                }
                let count = claim.min(operations - sequence) as usize;
                let began = Instant::now();
                let results = if config.workload == Workload::Write {
                    write_batch(&target, &config, sequence, count, phase).await
                } else {
                    let request_id = format!("{phase}-{sequence:016x}");
                    vec![operate(&target, &config, sequence, &request_id).await]
                };
                let elapsed = began.elapsed();
                if config.workload == Workload::Write {
                    samples.record_batch(elapsed, results);
                } else {
                    for result in results {
                        samples.record(elapsed, result);
                    }
                }
            }
            samples
        }));
    }
    let mut combined = Samples::default();
    for worker in workers {
        match worker.await {
            Ok(samples) => combined.merge(samples),
            Err(error) => {
                combined.errors += 1;
                combined
                    .error_classes
                    .insert(format!("worker join: {error}"), 1);
            }
        }
    }
    (combined, start.elapsed())
}

async fn run_phase_runtime(
    runtime: Arc<NodeRuntime>,
    config: &Config,
    operations: u64,
    phase: &'static str,
) -> (Samples, Duration) {
    let counter = Arc::new(AtomicU64::new(0));
    let start = Instant::now() + Duration::from_millis(20);
    let mut workers = Vec::with_capacity(config.concurrency);
    for _ in 0..config.concurrency {
        let runtime = Arc::clone(&runtime);
        let counter = Arc::clone(&counter);
        let config = config.clone();
        workers.push(tokio::task::spawn_blocking(move || {
            std::thread::sleep(start.saturating_duration_since(Instant::now()));
            let mut samples = Samples::default();
            loop {
                let claim = if config.workload == Workload::Write {
                    config.batch_size as u64
                } else {
                    1
                };
                let sequence = counter.fetch_add(claim, Ordering::Relaxed);
                if sequence >= operations {
                    break;
                }
                let count = claim.min(operations - sequence) as usize;
                let began = Instant::now();
                let results = if config.workload == Workload::Write {
                    write_batch_runtime(&runtime, &config, sequence, count, phase)
                } else {
                    let request_id = format!("{phase}-{sequence:016x}");
                    vec![operate_runtime(&runtime, &config, sequence, &request_id)]
                };
                let elapsed = began.elapsed();
                if config.workload == Workload::Write {
                    samples.record_batch(elapsed, results);
                } else {
                    for result in results {
                        samples.record(elapsed, result);
                    }
                }
            }
            samples
        }));
    }
    let mut combined = Samples::default();
    for worker in workers {
        match worker.await {
            Ok(samples) => combined.merge(samples),
            Err(error) => {
                combined.errors += 1;
                combined
                    .error_classes
                    .insert(format!("runtime worker join: {error}"), 1);
            }
        }
    }
    (combined, start.elapsed())
}

fn operate_runtime(
    runtime: &NodeRuntime,
    config: &Config,
    sequence: u64,
    request_id: &str,
) -> Result<(), String> {
    let key_index = sequence % KEYSPACE;
    match config.workload {
        Workload::Write => write_one_runtime(
            runtime,
            config.profile,
            key_index,
            request_id,
            config.value_bytes,
        ),
        Workload::Get => get_one_runtime(runtime, config, key_index),
        Workload::DocumentGet => get_graph_document_runtime(runtime, config, key_index),
        Workload::NativeRead => native_read_runtime(runtime, config),
    }
}

fn write_one_runtime(
    runtime: &NodeRuntime,
    profile: Profile,
    key_index: u64,
    request_id: &str,
    value_bytes: usize,
) -> Result<(), String> {
    let key = key(key_index);
    let value = value(key_index, request_id, value_bytes);
    match profile {
        Profile::Sql => runtime
            .execute_sql(SqlCommand {
                request_id: request_id.into(),
                statements: vec![SqlStatement {
                    sql: "INSERT INTO bench_items(key, value) VALUES (?1, ?2) \
                          ON CONFLICT(key) DO UPDATE SET value = excluded.value"
                        .into(),
                    parameters: vec![SqlValue::Text(key), SqlValue::Text(value)],
                }],
            })
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Profile::Graph => runtime
            .mutate_graph(
                GraphCommandV1::put_document(request_id, key, GraphValueV1::String(value))
                    .map_err(|error| error.to_string())?,
            )
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Profile::Kv => runtime
            .mutate_kv(
                KvCommandV1::put(request_id, key.into_bytes(), value.into_bytes())
                    .map_err(|error| error.to_string())?,
            )
            .map(|_| ())
            .map_err(|error| error.to_string()),
    }
}

fn write_batch_runtime(
    runtime: &NodeRuntime,
    config: &Config,
    first_sequence: u64,
    count: usize,
    phase: &str,
) -> Vec<Result<(), String>> {
    match config.profile {
        Profile::Sql => logical_batch_results(
            count,
            runtime.execute_sql_batch(
                (0..count)
                    .map(|offset| {
                        sql_write_command(first_sequence + offset as u64, phase, config.value_bytes)
                    })
                    .collect(),
            ),
        ),
        Profile::Graph => match (0..count)
            .map(|offset| {
                graph_write_command(first_sequence + offset as u64, phase, config.value_bytes)
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(commands) => logical_batch_results(count, runtime.mutate_graph_batch(commands)),
            Err(error) => repeated_batch_error(count, error),
        },
        Profile::Kv => match (0..count)
            .map(|offset| {
                kv_write_command(first_sequence + offset as u64, phase, config.value_bytes)
            })
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(commands) => logical_batch_results(count, runtime.mutate_kv_batch(commands)),
            Err(error) => repeated_batch_error(count, error),
        },
    }
}

fn get_one_runtime(runtime: &NodeRuntime, config: &Config, key_index: u64) -> Result<(), String> {
    let key = key(key_index);
    let expected = setup_value(key_index, config.value_bytes);
    let consistency = config.read_consistency.runtime();
    let tip = match config.profile {
        Profile::Sql => {
            let result = runtime
                .query_sql(
                    &SqlStatement {
                        sql: "SELECT value FROM bench_items WHERE key = ?1 LIMIT 1".into(),
                        parameters: vec![SqlValue::Text(key)],
                    },
                    consistency,
                    1,
                )
                .map_err(|error| error.to_string())?;
            if result.rows != vec![vec![SqlValue::Text(expected)]] {
                return Err("runtime SQL get returned an unexpected value".into());
            }
            (result.applied_index, result.hash)
        }
        Profile::Graph => {
            let result = runtime
                .query_graph(
                    "MATCH (d:RhizaDocument) WHERE d.id = $id \
                 RETURN d.string_value AS value LIMIT 1",
                    &BTreeMap::from([("id".into(), GraphParameterValue::String(key))]),
                    consistency,
                    1,
                )
                .map_err(|error| error.to_string())?;
            if result.rows != vec![vec![GraphResultValue::String(expected)]] {
                return Err("runtime graph get returned an unexpected value".into());
            }
            (result.applied_index, result.hash)
        }
        Profile::Kv => {
            let result = runtime
                .get_kv(key.as_bytes(), consistency)
                .map_err(|error| error.to_string())?;
            if result.value.as_deref() != Some(expected.as_bytes()) {
                return Err("runtime KV get returned an unexpected value".into());
            }
            (result.applied_index, result.hash)
        }
    };
    validate_read_tip(config.profile, tip.0, tip.1)
}

fn get_graph_document_runtime(
    runtime: &NodeRuntime,
    config: &Config,
    key_index: u64,
) -> Result<(), String> {
    if config.profile != Profile::Graph {
        return Err("document get requires graph profile".into());
    }
    let result = runtime
        .get_graph_document(&key(key_index), config.read_consistency.runtime())
        .map_err(|error| error.to_string())?;
    if result.value
        != Some(GraphValueV1::String(setup_value(
            key_index,
            config.value_bytes,
        )))
    {
        return Err("runtime document get returned an unexpected value".into());
    }
    validate_read_tip(config.profile, result.applied_index, result.hash)
}

fn native_read_runtime(runtime: &NodeRuntime, config: &Config) -> Result<(), String> {
    let consistency = config.read_consistency.runtime();
    let expected = expected_native_keys();
    let tip = match config.profile {
        Profile::Sql => {
            let result = runtime
                .query_sql(
                    &SqlStatement {
                        sql: "SELECT key FROM bench_items ORDER BY key LIMIT 16".into(),
                        parameters: vec![],
                    },
                    consistency,
                    16,
                )
                .map_err(|error| error.to_string())?;
            let actual = result
                .rows
                .into_iter()
                .map(|row| row.into_iter().next())
                .collect::<Option<Vec<_>>>();
            if actual != Some(expected.iter().cloned().map(SqlValue::Text).collect()) {
                return Err("runtime SQL native read returned unexpected keys".into());
            }
            (result.applied_index, result.hash)
        }
        Profile::Graph => {
            let result = runtime
                .query_graph(
                    "MATCH (d:RhizaDocument) RETURN d.id AS id ORDER BY id LIMIT 16",
                    &BTreeMap::new(),
                    consistency,
                    16,
                )
                .map_err(|error| error.to_string())?;
            let actual = result
                .rows
                .into_iter()
                .map(|row| row.into_iter().next())
                .collect::<Option<Vec<_>>>();
            if actual
                != Some(
                    expected
                        .iter()
                        .cloned()
                        .map(GraphResultValue::String)
                        .collect(),
                )
            {
                return Err("runtime graph native read returned unexpected keys".into());
            }
            (result.applied_index, result.hash)
        }
        Profile::Kv => {
            let result = runtime
                .scan_kv_prefix(b"bench-key-", 16, None, consistency)
                .map_err(|error| error.to_string())?;
            let actual: Vec<_> = result.rows().iter().map(|row| row.key().to_vec()).collect();
            if actual
                != expected
                    .iter()
                    .map(|key| key.as_bytes().to_vec())
                    .collect::<Vec<_>>()
            {
                return Err("runtime KV native read returned unexpected keys".into());
            }
            let tip = result.tip();
            (tip.applied_index(), tip.applied_hash())
        }
    };
    validate_read_tip(config.profile, tip.0, tip.1)
}

async fn operate(
    target: &Target,
    config: &Config,
    sequence: u64,
    request_id: &str,
) -> Result<(), String> {
    let key_index = sequence % KEYSPACE;
    match config.workload {
        Workload::Write => {
            write_one(
                target,
                config.profile,
                key_index,
                request_id,
                config.value_bytes,
            )
            .await
        }
        Workload::Get => get_one(target, config, key_index).await,
        Workload::DocumentGet => get_graph_document(target, config, key_index).await,
        Workload::NativeRead => native_read(target, config).await,
    }
}

async fn write_one(
    target: &Target,
    profile: Profile,
    key_index: u64,
    request_id: &str,
    value_bytes: usize,
) -> Result<(), String> {
    let key = key(key_index);
    let value = value(key_index, request_id, value_bytes);
    match profile {
        Profile::Sql => {
            execute_sql(
                target,
                SqlCommand {
                    request_id: request_id.into(),
                    statements: vec![SqlStatement {
                        sql: "INSERT INTO bench_items(key, value) VALUES (?1, ?2) \
                              ON CONFLICT(key) DO UPDATE SET value = excluded.value"
                            .into(),
                        parameters: vec![SqlValue::Text(key), SqlValue::Text(value)],
                    }],
                },
            )
            .await?;
        }
        Profile::Graph => {
            let command =
                GraphCommandV1::put_document(request_id, key, GraphValueV1::String(value))
                    .map_err(|error| error.to_string())?;
            match target {
                Target::Handle(handle) => {
                    handle
                        .mutate_graph(command)
                        .await
                        .map_err(|error| error.to_string())?;
                }
                Target::Runtime(runtime) => {
                    runtime_call(runtime, move |runtime| {
                        runtime.mutate_graph(command).map(|_| ())
                    })
                    .await?;
                }
            }
        }
        Profile::Kv => match target {
            Target::Handle(handle) => {
                    handle
                        .kv_put(key.into_bytes(), value.into_bytes(), request_id.to_string())
                        .await
                    .map_err(|error| error.to_string())?;
            }
            Target::Runtime(runtime) => {
                let command = KvCommandV1::put(request_id, key.into_bytes(), value.into_bytes())
                    .map_err(|error| error.to_string())?;
                runtime_call(runtime, move |runtime| {
                    runtime.mutate_kv(command).map(|_| ())
                })
                .await?;
            }
        },
    }
    Ok(())
}

async fn write_batch(
    target: &Target,
    config: &Config,
    first_sequence: u64,
    count: usize,
    phase: &'static str,
) -> Vec<Result<(), String>> {
    match (target, config.profile) {
        (Target::Handle(handle), Profile::Sql) => logical_batch_results(
            count,
            handle
                .execute_sql_batch(
                    (0..count)
                        .map(|offset| {
                            sql_write_command(
                                first_sequence + offset as u64,
                                phase,
                                config.value_bytes,
                            )
                        })
                        .collect(),
                )
                .await,
        ),
        (Target::Handle(handle), Profile::Graph) => {
            let commands = (0..count)
                .map(|offset| {
                    graph_write_command(first_sequence + offset as u64, phase, config.value_bytes)
                })
                .collect::<Result<Vec<_>, _>>();
            match commands {
                Ok(commands) => {
                    logical_batch_results(count, handle.mutate_graph_batch(commands).await)
                }
                Err(error) => repeated_batch_error(count, error),
            }
        }
        (Target::Handle(handle), Profile::Kv) => {
            let commands = (0..count)
                .map(|offset| {
                    kv_write_command(first_sequence + offset as u64, phase, config.value_bytes)
                })
                .collect::<Result<Vec<_>, _>>();
            match commands {
                Ok(commands) => {
                    logical_batch_results(count, handle.kv_batch(commands).await)
                }
                Err(error) => repeated_batch_error(count, error),
            }
        }
        (Target::Runtime(runtime), _) => {
            let runtime = Arc::clone(runtime);
            let config = config.clone();
            match tokio::task::spawn_blocking(move || {
                write_batch_runtime(&runtime, &config, first_sequence, count, phase)
            })
            .await
            {
                Ok(results) => results,
                Err(error) => repeated_batch_error(count, format!("runtime worker join: {error}")),
            }
        }
    }
}

fn sql_write_command(sequence: u64, phase: &str, value_bytes: usize) -> SqlCommand {
    let request_id = format!("{phase}-{sequence:016x}");
    let key = key(sequence % KEYSPACE);
    let value = value(sequence % KEYSPACE, &request_id, value_bytes);
    SqlCommand {
        request_id,
        statements: vec![SqlStatement {
            sql: "INSERT INTO bench_items(key, value) VALUES (?1, ?2) \
                  ON CONFLICT(key) DO UPDATE SET value = excluded.value"
                .into(),
            parameters: vec![SqlValue::Text(key), SqlValue::Text(value)],
        }],
    }
}

fn graph_write_command(
    sequence: u64,
    phase: &str,
    value_bytes: usize,
) -> Result<GraphCommandV1, String> {
    let request_id = format!("{phase}-{sequence:016x}");
    let key_index = sequence % KEYSPACE;
    GraphCommandV1::put_document(
        request_id.clone(),
        key(key_index),
        GraphValueV1::String(value(key_index, &request_id, value_bytes)),
    )
    .map_err(|error| error.to_string())
}

fn kv_write_command(sequence: u64, phase: &str, value_bytes: usize) -> Result<KvCommandV1, String> {
    let request_id = format!("{phase}-{sequence:016x}");
    let key_index = sequence % KEYSPACE;
    KvCommandV1::put(
        request_id.clone(),
        key(key_index).into_bytes(),
        value(key_index, &request_id, value_bytes).into_bytes(),
    )
    .map_err(|error| error.to_string())
}

fn logical_batch_results<T, ItemError, BatchError>(
    count: usize,
    result: Result<Vec<Result<T, ItemError>>, BatchError>,
) -> Vec<Result<(), String>>
where
    ItemError: ToString,
    BatchError: ToString,
{
    match result {
        Ok(results) if results.len() == count => results
            .into_iter()
            .map(|result| result.map(|_| ()).map_err(|error| error.to_string()))
            .collect(),
        Ok(results) => repeated_batch_error(
            count,
            format!(
                "batch returned {} results for {count} operations",
                results.len()
            ),
        ),
        Err(error) => repeated_batch_error(count, error.to_string()),
    }
}

fn repeated_batch_error(count: usize, error: impl Into<String>) -> Vec<Result<(), String>> {
    let error = error.into();
    (0..count).map(|_| Err(error.clone())).collect()
}

async fn execute_sql(target: &Target, command: SqlCommand) -> Result<(), String> {
    match target {
        Target::Handle(handle) => handle
            .execute_sql(command)
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Target::Runtime(runtime) => {
            runtime_call(runtime, move |runtime| {
                runtime.execute_sql(command).map(|_| ())
            })
            .await
        }
    }
}

async fn runtime_call<T, E, F>(runtime: &Arc<NodeRuntime>, operation: F) -> Result<T, String>
where
    T: Send + 'static,
    E: ToString + Send + 'static,
    F: FnOnce(Arc<NodeRuntime>) -> Result<T, E> + Send + 'static,
{
    let runtime = Arc::clone(runtime);
    tokio::task::spawn_blocking(move || operation(runtime))
        .await
        .map_err(|error| format!("runtime worker join: {error}"))?
        .map_err(|error| error.to_string())
}

async fn get_one(target: &Target, config: &Config, key_index: u64) -> Result<(), String> {
    let key = key(key_index);
    let expected = setup_value(key_index, config.value_bytes);
    let consistency = config.read_consistency.runtime();
    let tip = match config.profile {
        Profile::Sql => {
            let statement = SqlStatement {
                sql: "SELECT value FROM bench_items WHERE key = ?1 LIMIT 1".into(),
                parameters: vec![SqlValue::Text(key)],
            };
            let result = match target {
                Target::Handle(handle) => handle
                    .query(statement, consistency, 1)
                    .await
                    .map_err(|error| error.to_string())?,
                Target::Runtime(runtime) => {
                    runtime_call(runtime, move |runtime| {
                        runtime.query_sql(&statement, consistency, 1)
                    })
                    .await?
                }
            };
            if result.rows != vec![vec![SqlValue::Text(expected)]] {
                return Err("SQL get returned an unexpected value".into());
            }
            (result.applied_index, result.hash)
        }
        Profile::Graph => {
            let statement = "MATCH (d:RhizaDocument) WHERE d.id = $id \
                             RETURN d.string_value AS value LIMIT 1";
            let parameters = BTreeMap::from([("id".into(), GraphParameterValue::String(key))]);
            let result = match target {
                Target::Handle(handle) => handle
                    .query_graph(statement, parameters, consistency, 1)
                    .await
                    .map_err(|error| error.to_string())?,
                Target::Runtime(runtime) => {
                    runtime_call(runtime, move |runtime| {
                        runtime.query_graph(statement, &parameters, consistency, 1)
                    })
                    .await?
                }
            };
            if result.rows != vec![vec![GraphResultValue::String(expected)]] {
                return Err("graph get returned an unexpected value".into());
            }
            (result.applied_index, result.hash)
        }
        Profile::Kv => {
            let result = match target {
                Target::Handle(handle) => handle
                    .kv_get(key.as_bytes(), consistency)
                    .await
                    .map_err(|error| error.to_string())?,
                Target::Runtime(runtime) => {
                    let key = key.into_bytes();
                    runtime_call(runtime, move |runtime| runtime.get_kv(&key, consistency)).await?
                }
            };
            if result.value.as_deref() != Some(expected.as_bytes()) {
                return Err("KV get returned an unexpected value".into());
            }
            (result.applied_index, result.hash)
        }
    };
    validate_read_tip(config.profile, tip.0, tip.1)
}

async fn get_graph_document(
    target: &Target,
    config: &Config,
    key_index: u64,
) -> Result<(), String> {
    if config.profile != Profile::Graph {
        return Err("document get requires graph profile".into());
    }
    let id = key(key_index);
    let consistency = config.read_consistency.runtime();
    let result = match target {
        Target::Handle(handle) => handle
            .get_graph_document(id, consistency)
            .await
            .map_err(|error| error.to_string())?,
        Target::Runtime(runtime) => {
            runtime_call(runtime, move |runtime| {
                runtime.get_graph_document(&id, consistency)
            })
            .await?
        }
    };
    if result.value
        != Some(GraphValueV1::String(setup_value(
            key_index,
            config.value_bytes,
        )))
    {
        return Err("document get returned an unexpected value".into());
    }
    validate_read_tip(config.profile, result.applied_index, result.hash)
}

async fn native_read(target: &Target, config: &Config) -> Result<(), String> {
    let consistency = config.read_consistency.runtime();
    let expected = expected_native_keys();
    let tip = match config.profile {
        Profile::Sql => {
            let statement = SqlStatement {
                sql: "SELECT key FROM bench_items ORDER BY key LIMIT 16".into(),
                parameters: vec![],
            };
            let result = match target {
                Target::Handle(handle) => handle
                    .query(statement, consistency, 16)
                    .await
                    .map_err(|error| error.to_string())?,
                Target::Runtime(runtime) => {
                    runtime_call(runtime, move |runtime| {
                        runtime.query_sql(&statement, consistency, 16)
                    })
                    .await?
                }
            };
            let actual = result
                .rows
                .into_iter()
                .map(|row| row.into_iter().next())
                .collect::<Option<Vec<_>>>();
            if actual != Some(expected.iter().cloned().map(SqlValue::Text).collect()) {
                return Err("SQL native read returned unexpected keys".into());
            }
            (result.applied_index, result.hash)
        }
        Profile::Graph => {
            let statement = "MATCH (d:RhizaDocument) RETURN d.id AS id ORDER BY id LIMIT 16";
            let parameters = BTreeMap::new();
            let result = match target {
                Target::Handle(handle) => handle
                    .query_graph(statement, parameters, consistency, 16)
                    .await
                    .map_err(|error| error.to_string())?,
                Target::Runtime(runtime) => {
                    runtime_call(runtime, move |runtime| {
                        runtime.query_graph(statement, &parameters, consistency, 16)
                    })
                    .await?
                }
            };
            let actual = result
                .rows
                .into_iter()
                .map(|row| row.into_iter().next())
                .collect::<Option<Vec<_>>>();
            if actual
                != Some(
                    expected
                        .iter()
                        .cloned()
                        .map(GraphResultValue::String)
                        .collect(),
                )
            {
                return Err("graph native read returned unexpected keys".into());
            }
            (result.applied_index, result.hash)
        }
        Profile::Kv => {
            let result = match target {
                Target::Handle(handle) => handle
                    .kv_scan_prefix(b"bench-key-".to_vec(), 16, None, consistency)
                    .await
                    .map_err(|error| error.to_string())?,
                Target::Runtime(runtime) => {
                    runtime_call(runtime, move |runtime| {
                        runtime.scan_kv_prefix(b"bench-key-", 16, None, consistency)
                    })
                    .await?
                }
            };
            let actual: Vec<_> = result.rows().iter().map(|row| row.key().to_vec()).collect();
            if actual
                != expected
                    .iter()
                    .map(|key| key.as_bytes().to_vec())
                    .collect::<Vec<_>>()
            {
                return Err("KV native read returned unexpected keys".into());
            }
            let tip = result.tip();
            (tip.applied_index(), tip.applied_hash())
        }
    };
    validate_read_tip(config.profile, tip.0, tip.1)
}

enum RawState {
    Sql(Box<SqliteStateMachine>),
    Graph(LadybugStateMachine),
    Kv(RedbStateMachine),
}

struct RawTarget {
    cluster_id: String,
    next_index: u64,
    previous_hash: LogHash,
    state: RawState,
}

struct QwalBatchOutcome {
    results: Vec<Result<(), String>>,
    envelope_bytes: Option<usize>,
    prepare_latency: Duration,
    apply_latency: Option<Duration>,
    follower_apply_latency: Option<Duration>,
}

struct PreparedQwalBatch {
    results: Vec<Result<(), String>>,
    payload: Option<Vec<u8>>,
    envelope_bytes: Option<usize>,
    prepare_latency: Duration,
}

impl RawTarget {
    fn open(root: &Path, profile: Profile) -> Result<Self, String> {
        let cluster_id = effective_cluster_id(profile.execution_profile(), "profile-bench")
            .map_err(|error| error.to_string())?;
        let state = match profile {
            Profile::Sql => return Self::open_sql(root, "sql.db", "node-1"),
            Profile::Graph => {
                LadybugStateMachine::open(root.join("raw/graph.lbug"), &cluster_id, "node-1", 1, 1)
                    .map(RawState::Graph)
                    .map_err(|error| error.to_string())?
            }
            Profile::Kv => {
                RedbStateMachine::open(root.join("raw/kv.redb"), &cluster_id, "node-1", 1, 1)
                    .map(RawState::Kv)
                    .map_err(|error| error.to_string())?
            }
        };
        Ok(Self {
            cluster_id,
            next_index: 1,
            previous_hash: LogHash::ZERO,
            state,
        })
    }

    fn open_sql(root: &Path, database: &str, node_id: &str) -> Result<Self, String> {
        let cluster_id = effective_cluster_id(ExecutionProfile::Sqlite, "profile-bench")
            .map_err(|error| error.to_string())?;
        let state =
            SqliteStateMachine::open(root.join("raw").join(database), &cluster_id, node_id, 1, 1)
                .map(Box::new)
                .map_err(|error| error.to_string())?;
        let (applied_index, applied_hash) = state
            .applied_tip_value()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            cluster_id,
            next_index: applied_index
                .checked_add(1)
                .ok_or_else(|| "QWAL benchmark index exhausted".to_string())?,
            previous_hash: applied_hash,
            state: RawState::Sql(state),
        })
    }

    fn sql_state(&self) -> Result<&SqliteStateMachine, String> {
        match &self.state {
            RawState::Sql(state) => Ok(state),
            _ => Err("QWAL benchmark requires SQL state".into()),
        }
    }

    fn build_entry(&self, payload: Vec<u8>) -> LogEntry {
        let hash = LogEntry::calculate_hash(
            &self.cluster_id,
            self.next_index,
            1,
            1,
            EntryType::Command,
            self.previous_hash,
            &payload,
        );
        LogEntry {
            cluster_id: self.cluster_id.clone(),
            epoch: 1,
            config_id: 1,
            index: self.next_index,
            entry_type: EntryType::Command,
            payload,
            prev_hash: self.previous_hash,
            hash,
        }
    }

    fn apply_prebuilt_entry(&mut self, entry: &LogEntry) -> Result<(), String> {
        if entry.cluster_id != self.cluster_id
            || entry.index != self.next_index
            || entry.prev_hash != self.previous_hash
        {
            return Err("prebuilt benchmark entry does not extend the target anchor".into());
        }
        match &self.state {
            RawState::Sql(state) => state
                .apply_entry(entry)
                .map(|_| ())
                .map_err(|error| error.to_string()),
            RawState::Graph(state) => state
                .apply_entry(entry)
                .map(|_| ())
                .map_err(|error| error.to_string()),
            RawState::Kv(state) => state
                .apply_entry(entry)
                .map(|_| ())
                .map_err(|error| error.to_string()),
        }?;
        self.advance_anchor(entry)?;
        Ok(())
    }

    fn advance_anchor(&mut self, entry: &LogEntry) -> Result<(), String> {
        self.next_index = entry
            .index
            .checked_add(1)
            .ok_or_else(|| "QWAL benchmark index exhausted".to_string())?;
        self.previous_hash = entry.hash;
        Ok(())
    }

    fn apply(&mut self, payload: Vec<u8>) -> Result<(), String> {
        let entry = self.build_entry(payload);
        self.apply_prebuilt_entry(&entry)
    }

    fn prepare_sql_batch(&mut self, commands: &[SqlCommand]) -> Result<PreparedQwalBatch, String> {
        let requests = commands
            .iter()
            .map(encode_sql_command)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        let members = commands
            .iter()
            .zip(&requests)
            .map(|(command, request_payload)| SqlBatchMember {
                command,
                request_payload,
            })
            .collect::<Vec<_>>();
        let base_index = self
            .next_index
            .checked_sub(1)
            .ok_or_else(|| "QWAL benchmark index underflow".to_string())?;
        let prepare_began = Instant::now();
        let SqlBatchPreparation { effect, results } = {
            let RawState::Sql(state) = &self.state else {
                return Err("QWAL benchmark requires SQL state".into());
            };
            state
                .prepare_sql_batch_effect(&members, base_index, self.previous_hash)
                .map_err(|error| error.to_string())?
        };
        let prepare_latency = prepare_began.elapsed();
        let envelope_bytes = effect.as_ref().map(Vec::len);
        if let Some(payload) = &effect {
            if !payload.starts_with(QWAL_V3_MAGIC) {
                return Err("SQLite prepared a non-QWAL v3 benchmark effect".into());
            }
        }
        Ok(PreparedQwalBatch {
            results: results
                .into_iter()
                .map(|result| result.map(|_| ()).map_err(|error| error.to_string()))
                .collect(),
            payload: effect,
            envelope_bytes,
            prepare_latency,
        })
    }

    fn prepare_and_apply_sql_batch(
        &mut self,
        commands: &[SqlCommand],
    ) -> Result<QwalBatchOutcome, String> {
        let prepared = self.prepare_sql_batch(commands)?;
        let apply_latency = if let Some(payload) = prepared.payload {
            let apply_began = Instant::now();
            self.apply(payload)?;
            Some(apply_began.elapsed())
        } else {
            None
        };
        Ok(QwalBatchOutcome {
            results: prepared.results,
            envelope_bytes: prepared.envelope_bytes,
            prepare_latency: prepared.prepare_latency,
            apply_latency,
            follower_apply_latency: None,
        })
    }

    fn prepare_and_apply_sql(&mut self, command: &SqlCommand) -> Result<(), String> {
        let mut outcome = self.prepare_and_apply_sql_batch(std::slice::from_ref(command))?;
        outcome
            .results
            .pop()
            .expect("one-member SQL batch returns one result")
    }

    fn write_sql_qwal(
        &mut self,
        key_index: u64,
        request_id: &str,
        value_bytes: usize,
    ) -> Result<(), String> {
        let command = SqlCommand {
            request_id: request_id.into(),
            statements: vec![SqlStatement {
                sql: "INSERT INTO bench_items(key, value) VALUES (?1, ?2) \
                      ON CONFLICT(key) DO UPDATE SET value = excluded.value"
                    .into(),
                parameters: vec![
                    SqlValue::Text(key(key_index)),
                    SqlValue::Text(value(key_index, request_id, value_bytes)),
                ],
            }],
        };
        self.prepare_and_apply_sql(&command)
    }

    fn write_sql_qwal_batch(
        &mut self,
        first_sequence: u64,
        count: usize,
        phase: &str,
        value_bytes: usize,
    ) -> Result<QwalBatchOutcome, String> {
        let commands = (0..count)
            .map(|offset| sql_write_command(first_sequence + offset as u64, phase, value_bytes))
            .collect::<Vec<_>>();
        self.prepare_and_apply_sql_batch(&commands)
    }

    fn write_one(
        &mut self,
        profile: Profile,
        key_index: u64,
        request_id: &str,
        value_bytes: usize,
    ) -> Result<(), String> {
        let key = key(key_index);
        let value = value(key_index, request_id, value_bytes);
        let payload = match profile {
            Profile::Sql => {
                return Err("raw SQL writes require prepared QWAL v3 effects".into());
            }
            Profile::Graph => encode_replicated_graph_command(
                &GraphCommandV1::put_document(request_id, key, GraphValueV1::String(value))
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?,
            Profile::Kv => encode_replicated_kv_command(
                &KvCommandV1::put(request_id, key.into_bytes(), value.into_bytes())
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?,
        };
        self.apply(payload)
    }

    fn operate(&mut self, config: &Config, sequence: u64, request_id: &str) -> Result<(), String> {
        let key_index = sequence % KEYSPACE;
        match config.workload {
            Workload::Write if config.layer == Layer::Qwal => {
                self.write_sql_qwal(key_index, request_id, config.value_bytes)
            }
            Workload::Write => {
                self.write_one(config.profile, key_index, request_id, config.value_bytes)
            }
            Workload::Get => self.get_one(config.profile, key_index),
            Workload::DocumentGet => self.get_graph_document(config.profile, key_index),
            Workload::NativeRead => self.native_read(config.profile),
        }
    }

    fn get_one(&self, profile: Profile, key_index: u64) -> Result<(), String> {
        let key = key(key_index);
        let rows = match (&self.state, profile) {
            (RawState::Sql(state), Profile::Sql) => state
                .query_sql(
                    &SqlStatement {
                        sql: "SELECT value FROM bench_items WHERE key = ?1 LIMIT 1".into(),
                        parameters: vec![SqlValue::Text(key)],
                    },
                    1,
                    RAW_RESULT_BYTES,
                )
                .map_err(|error| error.to_string())?
                .rows
                .len(),
            (RawState::Graph(state), Profile::Graph) => state
                .query_read_only(
                    "MATCH (d:RhizaDocument) WHERE d.id = $id \
                     RETURN d.string_value AS value LIMIT 1",
                    &BTreeMap::from([("id".into(), GraphParameterValue::String(key))]),
                    1,
                    RAW_RESULT_BYTES,
                    RAW_GRAPH_TIMEOUT_MS,
                )
                .map_err(|error| error.to_string())?
                .rows
                .len(),
            (RawState::Kv(state), Profile::Kv) => usize::from(
                state
                    .get_with_tip(key.as_bytes())
                    .map_err(|error| error.to_string())?
                    .value()
                    .is_some(),
            ),
            _ => return Err("raw benchmark profile/state mismatch".into()),
        };
        if rows == 1 {
            Ok(())
        } else {
            Err("raw get returned no row".into())
        }
    }

    fn get_graph_document(&self, profile: Profile, key_index: u64) -> Result<(), String> {
        if profile != Profile::Graph {
            return Err("document get requires graph profile".into());
        }
        let RawState::Graph(state) = &self.state else {
            return Err("raw benchmark profile/state mismatch".into());
        };
        if state
            .get_document_with_tip(&key(key_index))
            .map_err(|error| error.to_string())?
            .0
            .is_some()
        {
            Ok(())
        } else {
            Err("raw document get returned no value".into())
        }
    }

    fn native_read(&self, profile: Profile) -> Result<(), String> {
        let rows = match (&self.state, profile) {
            (RawState::Sql(state), Profile::Sql) => state
                .query_sql(
                    &SqlStatement {
                        sql: "SELECT key FROM bench_items ORDER BY key LIMIT 16".into(),
                        parameters: vec![],
                    },
                    16,
                    RAW_RESULT_BYTES,
                )
                .map_err(|error| error.to_string())?
                .rows
                .len(),
            (RawState::Graph(state), Profile::Graph) => state
                .query_read_only(
                    "MATCH (d:RhizaDocument) RETURN d.id AS id ORDER BY id LIMIT 16",
                    &BTreeMap::new(),
                    16,
                    RAW_RESULT_BYTES,
                    RAW_GRAPH_TIMEOUT_MS,
                )
                .map_err(|error| error.to_string())?
                .rows
                .len(),
            (RawState::Kv(state), Profile::Kv) => state
                .scan_prefix(b"bench-key-", 16, None)
                .map_err(|error| error.to_string())?
                .rows()
                .len(),
            _ => return Err("raw benchmark profile/state mismatch".into()),
        };
        if rows == 0 {
            Err("raw native read returned no rows".into())
        } else {
            Ok(())
        }
    }
}

struct SqlFollowerTarget {
    leader: RawTarget,
    follower: RawTarget,
    leader_path: PathBuf,
    follower_path: PathBuf,
}

impl SqlFollowerTarget {
    fn seed(
        root: &Path,
        value_bytes: usize,
        padding_mib: usize,
        seed_cache: Option<&Path>,
    ) -> Result<(Self, FollowerSeedState), String> {
        let receipt_count = expected_seed_receipt_count(padding_mib)?;
        if let Some(cache_path) = seed_cache {
            let cache_path = absolute_cache_path(cache_path)?;
            if cache_path.try_exists().map_err(|error| error.to_string())? {
                let loaded =
                    load_sql_seed_cache(&cache_path, padding_mib, value_bytes, receipt_count)?;
                let (target, mut state) = Self::restore_compacted_seed(
                    root,
                    &loaded.snapshot,
                    value_bytes,
                    padding_mib,
                    receipt_count,
                )?;
                state.seed_cache = Some(loaded.provenance);
                return Ok((target, state));
            }
        }

        let mut seed_leader = RawTarget::open_sql(root, "leader-seed.db", "node-1")?;
        let mut receipt_count = 0u64;
        visit_seed_commands(value_bytes, padding_mib, |command| {
            apply_seed_command(&mut seed_leader, command)?;
            receipt_count += 1;
            Ok(())
        })?;
        let expected_receipts = expected_seed_receipt_count(padding_mib)?;
        if receipt_count != expected_receipts {
            return Err("SQL follower seed receipt count invariant failed".into());
        }
        let snapshot = seed_leader
            .sql_state()?
            .create_recovery_snapshot(1)
            .map_err(|error| error.to_string())?;
        Self::restore_validated_seed(
            root,
            snapshot.snapshot(),
            value_bytes,
            padding_mib,
            receipt_count,
            seed_cache,
        )
    }

    fn restore_validated_seed(
        root: &Path,
        snapshot: &Snapshot,
        value_bytes: usize,
        padding_mib: usize,
        receipt_count: u64,
        seed_cache: Option<&Path>,
    ) -> Result<(Self, FollowerSeedState), String> {
        let (target, mut state) =
            Self::restore_compacted_seed(root, snapshot, value_bytes, padding_mib, receipt_count)?;
        if let Some(cache_path) = seed_cache {
            state.seed_cache = Some(publish_sql_seed_cache(
                &absolute_cache_path(cache_path)?,
                snapshot,
                padding_mib,
                value_bytes,
                receipt_count,
            )?);
        }
        Ok((target, state))
    }

    fn restore_compacted_seed(
        root: &Path,
        snapshot: &Snapshot,
        value_bytes: usize,
        padding_mib: usize,
        receipt_count: u64,
    ) -> Result<(Self, FollowerSeedState), String> {
        let leader_path = root.join("raw/leader-measured.db");
        let follower_path = root.join("raw/follower-measured.db");
        restore_snapshot_file(&leader_path, snapshot, "node-1")
            .map_err(|error| error.to_string())?;
        restore_snapshot_file(&follower_path, snapshot, "node-2")
            .map_err(|error| error.to_string())?;

        let target = Self {
            leader: RawTarget::open_sql(root, "leader-measured.db", "node-1")?,
            follower: RawTarget::open_sql(root, "follower-measured.db", "node-2")?,
            leader_path,
            follower_path,
        };
        target.verify_aligned_anchor()?;

        verify_snapshot_cleared_embedded_qlog(target.leader.sql_state()?)?;
        verify_snapshot_cleared_embedded_qlog(target.follower.sql_state()?)?;
        target.validate_restored_seed(value_bytes, padding_mib, receipt_count)?;

        let state = FollowerSeedState {
            receipt_count,
            leader_database_bytes: file_bytes(&target.leader_path)?,
            follower_database_bytes: file_bytes(&target.follower_path)?,
            leader_control_bytes: file_bytes(&control_sidecar_path(&target.leader_path))?,
            follower_control_bytes: file_bytes(&control_sidecar_path(&target.follower_path))?,
            leader_embedded_qlog_entries: 0,
            follower_embedded_qlog_entries: 0,
            seed_cache: None,
        };
        Ok((target, state))
    }

    fn validate_restored_seed(
        &self,
        value_bytes: usize,
        padding_mib: usize,
        expected_receipts: u64,
    ) -> Result<(), String> {
        self.verify_aligned_anchor()?;
        let expected_tip = expected_receipts;
        if self.leader.next_index != expected_tip.saturating_add(1) {
            return Err("restored SQL seed tip does not match expected receipt count".into());
        }
        for state in [self.leader.sql_state()?, self.follower.sql_state()?] {
            if state
                .configuration_state_value()
                .map_err(|error| error.to_string())?
                != ConfigurationState::active(1, LogHash::ZERO)
            {
                return Err("restored SQL seed does not use the exact active configuration".into());
            }
            validate_seed_database(state, value_bytes, padding_mib)?;
        }
        let mut validated_receipts = 0u64;
        visit_seed_commands(value_bytes, padding_mib, |command| {
            let request_id = command.request_id.clone();
            let encoded = encode_sql_command(&command).map_err(|error| error.to_string())?;
            for state in [self.leader.sql_state()?, self.follower.sql_state()?] {
                let Some(outcome) = state
                    .check_request(&request_id, &encoded)
                    .map_err(|error| error.to_string())?
                else {
                    return Err(format!("restored SQL seed is missing receipt {request_id}"));
                };
                let expected_index = validated_receipts + 1;
                if outcome.original_log_index() != expected_index {
                    return Err(format!(
                        "restored SQL seed receipt {request_id} belongs to index {}, expected {expected_index}",
                        outcome.original_log_index()
                    ));
                }
            }
            validated_receipts += 1;
            Ok(())
        })?;
        if validated_receipts != expected_receipts {
            return Err(
                "restored SQL seed expected-receipt validation count does not match metadata"
                    .into(),
            );
        }
        Ok(())
    }

    fn verify_aligned_anchor(&self) -> Result<(), String> {
        if self.leader.next_index != self.follower.next_index
            || self.leader.previous_hash != self.follower.previous_hash
        {
            return Err("SQL follower benchmark leader and follower anchors diverged".into());
        }
        Ok(())
    }

    fn verify_identical_state(&self) -> Result<(), String> {
        let leader = self.leader.sql_state()?;
        let follower = self.follower.sql_state()?;
        if leader
            .canonical_db_digest()
            .map_err(|error| error.to_string())?
            != follower
                .canonical_db_digest()
                .map_err(|error| error.to_string())?
            || leader
                .applied_tip_value()
                .map_err(|error| error.to_string())?
                != follower
                    .applied_tip_value()
                    .map_err(|error| error.to_string())?
        {
            return Err("SQL follower benchmark leader and follower states diverged".into());
        }
        Ok(())
    }

    fn write_sql_qwal_batch(
        &mut self,
        first_sequence: u64,
        count: usize,
        phase: &str,
        value_bytes: usize,
    ) -> Result<QwalBatchOutcome, String> {
        let commands = (0..count)
            .map(|offset| sql_write_command(first_sequence + offset as u64, phase, value_bytes))
            .collect::<Vec<_>>();
        let prepared = self.leader.prepare_sql_batch(&commands)?;
        let follower_apply_latency = if let Some(payload) = prepared.payload {
            let follower_entry = self.follower.build_entry(payload);
            let leader_entry = follower_entry.clone();
            let follower_state = self.follower.sql_state()?;
            let follower_apply_began = Instant::now();
            let follower_apply = follower_state.apply_entry(&follower_entry);
            let follower_apply_latency = follower_apply_began.elapsed();
            follower_apply.map_err(|error| error.to_string())?;
            self.follower.advance_anchor(&follower_entry)?;
            self.leader.apply_prebuilt_entry(&leader_entry)?;
            self.verify_aligned_anchor()?;
            Some(follower_apply_latency)
        } else {
            None
        };
        Ok(QwalBatchOutcome {
            results: prepared.results,
            envelope_bytes: prepared.envelope_bytes,
            prepare_latency: prepared.prepare_latency,
            apply_latency: None,
            follower_apply_latency,
        })
    }
}

fn apply_seed_command(leader: &mut RawTarget, command: SqlCommand) -> Result<(), String> {
    let prepared = leader.prepare_sql_batch(&[command])?;
    if prepared.results.iter().any(Result::is_err) {
        return Err("SQL follower benchmark seed command failed".into());
    }
    let payload = prepared
        .payload
        .ok_or_else(|| "SQL follower benchmark seed produced no QWAL effect".to_string())?;
    leader.apply(payload)
}

fn expected_seed_receipt_count(padding_mib: usize) -> Result<u64, String> {
    let padding_bytes = padding_mib
        .checked_mul(1024 * 1024)
        .ok_or_else(|| "SQL padding byte count overflows".to_string())?;
    let padding_receipts = padding_bytes.div_ceil(SQL_PADDING_CHUNK_BYTES);
    u64::try_from(padding_receipts)
        .ok()
        .and_then(|count| count.checked_add(KEYSPACE + 1))
        .ok_or_else(|| "SQL seed receipt count overflows".to_string())
}

fn visit_seed_commands(
    value_bytes: usize,
    padding_mib: usize,
    mut visit: impl FnMut(SqlCommand) -> Result<(), String>,
) -> Result<(), String> {
    visit(SqlCommand {
        request_id: "profile-bench-schema".into(),
        statements: vec![
            SqlStatement {
                sql: SQL_SEED_ITEMS_DDL.into(),
                parameters: vec![],
            },
            SqlStatement {
                sql: SQL_SEED_PADDING_DDL.into(),
                parameters: vec![],
            },
        ],
    })?;

    let padding_bytes = padding_mib
        .checked_mul(1024 * 1024)
        .ok_or_else(|| "SQL padding byte count overflows".to_string())?;
    let mut inserted = 0usize;
    let mut chunk_id = 0u64;
    while inserted < padding_bytes {
        let bytes = SQL_PADDING_CHUNK_BYTES.min(padding_bytes - inserted);
        visit(SqlCommand {
            request_id: format!("profile-bench-padding-{chunk_id:016x}"),
            statements: vec![SqlStatement {
                sql: SQL_SEED_PADDING_INSERT.into(),
                parameters: vec![
                    SqlValue::Integer(
                        i64::try_from(chunk_id)
                            .map_err(|_| "SQL padding chunk id exceeds SQLite INTEGER")?,
                    ),
                    SqlValue::Blob(vec![0; bytes]),
                ],
            }],
        })?;
        inserted += bytes;
        chunk_id += 1;
    }
    for index in 0..KEYSPACE {
        visit(sql_write_command(index, "setup", value_bytes))?;
    }
    Ok(())
}

fn sql_seed_recipe_digest(value_bytes: usize, padding_mib: usize) -> Result<String, String> {
    let value_bytes = u64::try_from(value_bytes)
        .map_err(|_| "SQL seed recipe value size exceeds u64".to_string())?
        .to_be_bytes();
    let padding_mib = u64::try_from(padding_mib)
        .map_err(|_| "SQL seed recipe padding size exceeds u64".to_string())?
        .to_be_bytes();
    let keyspace = KEYSPACE.to_be_bytes();
    let padding_chunk_bytes = u64::try_from(SQL_PADDING_CHUNK_BYTES)
        .map_err(|_| "SQL seed recipe padding chunk size exceeds u64".to_string())?
        .to_be_bytes();
    Ok(LogHash::digest(&[
        SQL_SEED_RECIPE_ID.as_bytes(),
        SQL_SEED_ITEMS_DDL.as_bytes(),
        SQL_SEED_PADDING_DDL.as_bytes(),
        SQL_SEED_PADDING_INSERT.as_bytes(),
        b"sql_write_command:setup:v1",
        &keyspace,
        &padding_chunk_bytes,
        &padding_mib,
        &value_bytes,
    ])
    .to_hex())
}

fn validate_seed_database(
    state: &SqliteStateMachine,
    value_bytes: usize,
    padding_mib: usize,
) -> Result<(), String> {
    let expected_padding_rows = padding_mib
        .checked_mul(1024 * 1024)
        .map(|bytes| bytes.div_ceil(SQL_PADDING_CHUNK_BYTES))
        .ok_or_else(|| "SQL padding byte count overflows".to_string())?;
    let padding = state
        .query_sql(
            &SqlStatement {
                sql: "SELECT id, length(payload), \
                      CASE WHEN payload = zeroblob(length(payload)) THEN 1 ELSE 0 END \
                      FROM bench_padding ORDER BY id"
                    .into(),
                parameters: vec![],
            },
            expected_padding_rows.saturating_add(1),
            RAW_RESULT_BYTES,
        )
        .map_err(|error| error.to_string())?;
    validate_padding_rows(&padding.rows, padding_mib)?;

    let items = state
        .query_sql(
            &SqlStatement {
                sql: "SELECT key, value FROM bench_items ORDER BY key".into(),
                parameters: vec![],
            },
            KEYSPACE as usize,
            2 * RAW_RESULT_BYTES,
        )
        .map_err(|error| error.to_string())?;
    if items.rows.len() != KEYSPACE as usize {
        return Err(format!(
            "restored SQL seed has {} hot rows; expected {KEYSPACE}",
            items.rows.len()
        ));
    }
    for (index, row) in items.rows.iter().enumerate() {
        let expected_index = index as u64;
        let expected = [
            SqlValue::Text(key(expected_index)),
            SqlValue::Text(setup_value(expected_index, value_bytes)),
        ];
        if row != &expected {
            return Err(format!(
                "restored SQL seed hot row {expected_index} does not match --value-bytes"
            ));
        }
    }
    Ok(())
}

fn validate_padding_rows(rows: &[Vec<SqlValue>], padding_mib: usize) -> Result<(), String> {
    let padding_bytes = padding_mib
        .checked_mul(1024 * 1024)
        .ok_or_else(|| "SQL padding byte count overflows".to_string())?;
    let expected_rows = padding_bytes.div_ceil(SQL_PADDING_CHUNK_BYTES);
    if rows.len() != expected_rows {
        return Err(format!(
            "restored SQL seed has {} padding rows; expected {expected_rows}",
            rows.len()
        ));
    }
    for (index, row) in rows.iter().enumerate() {
        let offset = index
            .checked_mul(SQL_PADDING_CHUNK_BYTES)
            .ok_or_else(|| "SQL padding offset overflows".to_string())?;
        let expected_bytes = SQL_PADDING_CHUNK_BYTES.min(padding_bytes - offset);
        let expected = [
            SqlValue::Integer(
                i64::try_from(index)
                    .map_err(|_| "SQL padding row id exceeds SQLite INTEGER".to_string())?,
            ),
            SqlValue::Integer(
                i64::try_from(expected_bytes)
                    .map_err(|_| "SQL padding row size exceeds SQLite INTEGER".to_string())?,
            ),
            SqlValue::Integer(1),
        ];
        if row != &expected {
            return Err(format!(
                "restored SQL padding row {index} must have its exact id, size, and all-zero content"
            ));
        }
    }
    Ok(())
}

fn absolute_cache_path(path: &Path) -> Result<PathBuf, String> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        env::current_dir()
            .map(|current| current.join(path))
            .map_err(|error| format!("failed to resolve SQL seed cache path: {error}"))
    }
}

fn sql_seed_cache_header(
    snapshot: &Snapshot,
    padding_mib: usize,
    value_bytes: usize,
    receipt_count: u64,
) -> Result<SqlSeedCacheHeader, String> {
    if snapshot.manifest().index() != receipt_count {
        return Err("SQL seed snapshot index does not match receipt count".into());
    }
    let configuration_state = ConfigurationState::active(1, LogHash::ZERO);
    if snapshot.manifest().configuration_state() != &configuration_state {
        return Err("SQL seed snapshot does not use the exact active configuration".into());
    }
    Ok(SqlSeedCacheHeader {
        format_version: SQL_SEED_CACHE_FORMAT_VERSION,
        harness_version: SQL_SEED_CACHE_HARNESS_VERSION.into(),
        report_schema_version: REPORT_SCHEMA_VERSION,
        seed_recipe_id: SQL_SEED_RECIPE_ID.into(),
        seed_recipe_digest: sql_seed_recipe_digest(value_bytes, padding_mib)?,
        profile: "sql".into(),
        workload: "follower-apply-write".into(),
        padding_mib,
        value_bytes,
        keyspace: KEYSPACE,
        receipt_count,
        snapshot_bytes: u64::try_from(snapshot.db_bytes().len())
            .map_err(|_| "SQL seed snapshot size exceeds u64".to_string())?,
        snapshot_digest: LogHash::digest(&[snapshot.db_bytes()]).to_hex(),
        configuration_state,
        snapshot_manifest: snapshot.manifest().clone(),
    })
}

fn encode_sql_seed_cache_parts(
    header: &SqlSeedCacheHeader,
    snapshot_bytes: &[u8],
) -> Result<(Vec<u8>, [u8; 4], String, u64), String> {
    let header_bytes = serde_json::to_vec(header).map_err(|error| error.to_string())?;
    if header_bytes.len() > MAX_SQL_SEED_CACHE_HEADER_BYTES {
        return Err("SQL seed cache header exceeds its bounded size".into());
    }
    let header_len = u32::try_from(header_bytes.len())
        .map_err(|_| "SQL seed cache header length exceeds u32".to_string())?
        .to_be_bytes();
    let total_bytes = SQL_SEED_CACHE_MAGIC
        .len()
        .checked_add(header_len.len())
        .and_then(|bytes| bytes.checked_add(header_bytes.len()))
        .and_then(|bytes| bytes.checked_add(snapshot_bytes.len()))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| "SQL seed cache size overflows".to_string())?;
    let digest = LogHash::digest(&[
        SQL_SEED_CACHE_MAGIC,
        &header_len,
        &header_bytes,
        snapshot_bytes,
    ])
    .to_hex();
    Ok((header_bytes, header_len, digest, total_bytes))
}

fn sql_seed_cache_max_snapshot_bytes(
    padding_mib: usize,
    value_bytes: usize,
) -> Result<u64, String> {
    let padding_bytes = u64::try_from(
        padding_mib
            .checked_mul(1024 * 1024)
            .ok_or_else(|| "SQL seed cache padding size overflows".to_string())?,
    )
    .map_err(|_| "SQL seed cache padding size exceeds u64".to_string())?;
    let hot_value_bytes = KEYSPACE
        .checked_mul(
            u64::try_from(value_bytes)
                .map_err(|_| "SQL seed cache value size exceeds u64".to_string())?,
        )
        .ok_or_else(|| "SQL seed cache hot value size overflows".to_string())?;
    padding_bytes
        .checked_add(hot_value_bytes)
        .and_then(|bytes| bytes.checked_add(SQL_SEED_CACHE_FIXED_OVERHEAD_BYTES))
        .ok_or_else(|| "SQL seed cache snapshot bound overflows".to_string())
}

fn sql_seed_cache_max_file_bytes(padding_mib: usize, value_bytes: usize) -> Result<u64, String> {
    sql_seed_cache_max_snapshot_bytes(padding_mib, value_bytes)?
        .checked_add(SQL_SEED_CACHE_MAGIC.len() as u64)
        .and_then(|bytes| bytes.checked_add(4))
        .and_then(|bytes| bytes.checked_add(MAX_SQL_SEED_CACHE_HEADER_BYTES as u64))
        .ok_or_else(|| "SQL seed cache file bound overflows".to_string())
}

fn publish_sql_seed_cache(
    path: &Path,
    snapshot: &Snapshot,
    padding_mib: usize,
    value_bytes: usize,
    receipt_count: u64,
) -> Result<SqlSeedCacheProvenance, String> {
    let header = sql_seed_cache_header(snapshot, padding_mib, value_bytes, receipt_count)?;
    let (header_bytes, header_len, digest, bytes) =
        encode_sql_seed_cache_parts(&header, snapshot.db_bytes())?;
    if bytes > sql_seed_cache_max_file_bytes(padding_mib, value_bytes)? {
        return Err("generated SQL seed cache exceeds its configuration-derived size bound".into());
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|error| {
        format!(
            "failed to create SQL seed cache directory {}: {error}",
            parent.display()
        )
    })?;
    let canonical_parent = fs::canonicalize(parent).map_err(|error| {
        format!(
            "failed to canonicalize SQL seed cache directory {}: {error}",
            parent.display()
        )
    })?;
    let file_name = path
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "SQL seed cache requires a file name".to_string())?;
    let canonical_path = canonical_parent.join(file_name);
    let mut temporary = tempfile::NamedTempFile::new_in(&canonical_parent).map_err(|error| {
        format!(
            "failed to create SQL seed cache temporary file in {}: {error}",
            canonical_parent.display()
        )
    })?;
    temporary
        .write_all(SQL_SEED_CACHE_MAGIC)
        .and_then(|_| temporary.write_all(&header_len))
        .and_then(|_| temporary.write_all(&header_bytes))
        .and_then(|_| temporary.write_all(snapshot.db_bytes()))
        .and_then(|_| temporary.as_file().sync_all())
        .map_err(|error| format!("failed to write SQL seed cache: {error}"))?;
    match temporary.persist_noclobber(&canonical_path) {
        Ok(file) => {
            file.sync_all()
                .map_err(|error| format!("failed to sync SQL seed cache: {error}"))?;
            sync_cache_parent(&canonical_parent)?;
            Ok(SqlSeedCacheProvenance {
                disposition: "created",
                path: canonical_path.display().to_string(),
                digest,
                bytes,
                snapshot_digest: header.snapshot_digest,
            })
        }
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            let loaded =
                load_sql_seed_cache(&canonical_path, padding_mib, value_bytes, receipt_count)?;
            if loaded.provenance.digest != digest
                || loaded.provenance.snapshot_digest != header.snapshot_digest
                || loaded.provenance.bytes != bytes
            {
                return Err(
                    "concurrent SQL seed cache publication produced different exact bytes".into(),
                );
            }
            Ok(loaded.provenance)
        }
        Err(error) => Err(format!(
            "failed to publish SQL seed cache {}: {}",
            canonical_path.display(),
            error.error
        )),
    }
}

fn load_sql_seed_cache(
    path: &Path,
    padding_mib: usize,
    value_bytes: usize,
    receipt_count: u64,
) -> Result<LoadedSqlSeedCache, String> {
    let expected_recipe_digest = sql_seed_recipe_digest(value_bytes, padding_mib)?;
    let expected_configuration = ConfigurationState::active(1, LogHash::ZERO);
    let maximum_file_bytes = sql_seed_cache_max_file_bytes(padding_mib, value_bytes)?;
    let before = fs::symlink_metadata(path).map_err(|error| {
        format!(
            "failed to inspect SQL seed cache {}: {error}",
            path.display()
        )
    })?;
    if !before.file_type().is_file() || before.file_type().is_symlink() {
        return Err("SQL seed cache must be a regular non-symlink file".into());
    }
    if before.len() > maximum_file_bytes {
        return Err(format!(
            "SQL seed cache is {} bytes; configuration-derived maximum is {maximum_file_bytes}",
            before.len()
        ));
    }
    let mut file = fs::File::open(path)
        .map_err(|error| format!("failed to open SQL seed cache {}: {error}", path.display()))?;
    let opened = file
        .metadata()
        .map_err(|error| format!("failed to inspect opened SQL seed cache: {error}"))?;
    if !same_opened_file(&before, &opened) {
        return Err("SQL seed cache changed while it was being opened".into());
    }

    let minimum = SQL_SEED_CACHE_MAGIC.len() + 4;
    if opened.len() < minimum as u64 {
        return Err("SQL seed cache header is truncated".into());
    }
    let mut magic = vec![0; SQL_SEED_CACHE_MAGIC.len()];
    file.read_exact(&mut magic)
        .map_err(|error| format!("failed to read SQL seed cache magic: {error}"))?;
    if magic != SQL_SEED_CACHE_MAGIC {
        return Err("SQL seed cache magic is invalid".into());
    }
    let mut header_len_bytes = [0; 4];
    file.read_exact(&mut header_len_bytes)
        .map_err(|error| format!("failed to read SQL seed cache header length: {error}"))?;
    let header_len = u32::from_be_bytes(header_len_bytes) as usize;
    if header_len == 0 || header_len > MAX_SQL_SEED_CACHE_HEADER_BYTES {
        return Err("SQL seed cache header length is invalid".into());
    }
    let header_end = minimum
        .checked_add(header_len)
        .ok_or_else(|| "SQL seed cache header size overflows".to_string())?;
    if opened.len() < header_end as u64 {
        return Err("SQL seed cache header is truncated".into());
    }
    let mut header_bytes = vec![0; header_len];
    file.read_exact(&mut header_bytes)
        .map_err(|error| format!("failed to read SQL seed cache header: {error}"))?;
    let header: SqlSeedCacheHeader =
        serde_json::from_slice(&header_bytes).map_err(|error| error.to_string())?;
    if serde_json::to_vec(&header).map_err(|error| error.to_string())? != header_bytes {
        return Err("SQL seed cache header is not canonically encoded".into());
    }
    if header.format_version != SQL_SEED_CACHE_FORMAT_VERSION
        || header.harness_version != SQL_SEED_CACHE_HARNESS_VERSION
        || header.report_schema_version != REPORT_SCHEMA_VERSION
        || header.seed_recipe_id != SQL_SEED_RECIPE_ID
        || header.seed_recipe_digest != expected_recipe_digest
        || header.profile != "sql"
        || header.workload != "follower-apply-write"
        || header.padding_mib != padding_mib
        || header.value_bytes != value_bytes
        || header.keyspace != KEYSPACE
        || header.receipt_count != receipt_count
    {
        return Err("SQL seed cache metadata does not match this benchmark configuration".into());
    }
    let maximum_snapshot_bytes = sql_seed_cache_max_snapshot_bytes(padding_mib, value_bytes)?;
    if header.snapshot_bytes > maximum_snapshot_bytes {
        return Err("SQL seed cache snapshot exceeds its configuration-derived size bound".into());
    }
    let exact_file_bytes = (header_end as u64)
        .checked_add(header.snapshot_bytes)
        .ok_or_else(|| "SQL seed cache declared size overflows".to_string())?;
    if exact_file_bytes != opened.len() {
        return Err("SQL seed cache length does not match its canonical header".into());
    }
    if header.configuration_state != expected_configuration
        || header.snapshot_manifest.configuration_state() != &header.configuration_state
        || header.snapshot_manifest.index() != receipt_count
        || header.snapshot_manifest.epoch() != 1
        || header.snapshot_manifest.config_id() != 1
        || header.snapshot_manifest.schema_version() != 1
        || header.snapshot_manifest.created_by() != "node-1"
    {
        return Err("SQL seed cache snapshot identity or digest is invalid".into());
    }
    let expected_cluster = effective_cluster_id(ExecutionProfile::Sqlite, "profile-bench")
        .map_err(|error| error.to_string())?;
    if header.snapshot_manifest.cluster_id() != expected_cluster {
        return Err("SQL seed cache snapshot cluster identity is invalid".into());
    }
    let snapshot_len = usize::try_from(header.snapshot_bytes)
        .map_err(|_| "SQL seed cache snapshot size exceeds usize".to_string())?;
    let mut snapshot_bytes = Vec::new();
    snapshot_bytes
        .try_reserve_exact(snapshot_len)
        .map_err(|error| format!("failed to reserve SQL seed cache snapshot: {error}"))?;
    snapshot_bytes.resize(snapshot_len, 0);
    file.read_exact(&mut snapshot_bytes)
        .map_err(|error| format!("failed to read SQL seed cache snapshot: {error}"))?;
    if header.snapshot_digest != LogHash::digest(&[&snapshot_bytes]).to_hex() {
        return Err("SQL seed cache snapshot digest is invalid".into());
    }
    let (canonical_header, canonical_len, digest, bytes) =
        encode_sql_seed_cache_parts(&header, &snapshot_bytes)?;
    if canonical_header != header_bytes
        || canonical_len != header_len_bytes
        || bytes != opened.len()
    {
        return Err("SQL seed cache canonical framing is invalid".into());
    }
    let opened_after = file
        .metadata()
        .map_err(|error| format!("failed to reseal opened SQL seed cache: {error}"))?;
    if !same_opened_file(&opened, &opened_after) {
        return Err("SQL seed cache was rewritten while it was being read".into());
    }
    let after = fs::symlink_metadata(path)
        .map_err(|error| format!("failed to revalidate SQL seed cache path: {error}"))?;
    if !after.file_type().is_file()
        || after.file_type().is_symlink()
        || !same_opened_file(&opened_after, &after)
    {
        return Err("SQL seed cache path changed while it was being read".into());
    }
    let canonical_path = fs::canonicalize(path)
        .map_err(|error| format!("failed to canonicalize SQL seed cache path: {error}"))?;
    let canonical_metadata = fs::symlink_metadata(&canonical_path)
        .map_err(|error| format!("failed to inspect canonical SQL seed cache path: {error}"))?;
    if !same_opened_file(&opened_after, &canonical_metadata) {
        return Err("canonical SQL seed cache path does not identify the opened file".into());
    }
    Ok(LoadedSqlSeedCache {
        snapshot: Snapshot::new(header.snapshot_manifest, snapshot_bytes),
        provenance: SqlSeedCacheProvenance {
            disposition: "hit",
            path: canonical_path.display().to_string(),
            digest,
            bytes,
            snapshot_digest: header.snapshot_digest,
        },
    })
}

#[cfg(unix)]
fn same_opened_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    left.file_type().is_file()
        && right.file_type().is_file()
        && left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
        && left.ctime() == right.ctime()
        && left.ctime_nsec() == right.ctime_nsec()
}

#[cfg(not(unix))]
fn same_opened_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.file_type().is_file() && right.file_type().is_file() && left.len() == right.len()
}

#[cfg(unix)]
fn sync_cache_parent(parent: &Path) -> Result<(), String> {
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| format!("failed to sync SQL seed cache directory: {error}"))
}

#[cfg(not(unix))]
fn sync_cache_parent(_parent: &Path) -> Result<(), String> {
    Ok(())
}

fn control_sidecar_path(database: &Path) -> PathBuf {
    let mut path = database.as_os_str().to_os_string();
    path.push(".control");
    PathBuf::from(path)
}

fn file_bytes(path: &Path) -> Result<u64, String> {
    std::fs::metadata(path)
        .map(|metadata| metadata.len())
        .map_err(|error| format!("failed to inspect {}: {error}", path.display()))
}

fn verify_snapshot_cleared_embedded_qlog(state: &SqliteStateMachine) -> Result<(), String> {
    match state.embedded_log_entries(1, 1) {
        Err(error)
            if error
                .to_string()
                .contains("embedded qlog is missing index 1") =>
        {
            Ok(())
        }
        Ok(_) => Err("restored follower benchmark seed retained embedded qlog index 1".into()),
        Err(error) => Err(error.to_string()),
    }
}

fn measure_follower(
    root: &Path,
    config: &Config,
) -> Result<(Samples, Duration, FollowerSeedState), String> {
    let (mut target, seed_state) = SqlFollowerTarget::seed(
        root,
        config.value_bytes,
        config.sql_padding_mib,
        config.sql_seed_cache.as_deref(),
    )?;
    if config.warmup > 0 {
        let (warmup, _) = run_phase_follower(&mut target, config, config.warmup, "warmup");
        if warmup.errors > 0 {
            return Err(format!("warmup failed with {} errors", warmup.errors));
        }
        target.verify_aligned_anchor()?;
    }
    let measured = run_phase_follower(&mut target, config, config.operations, "measure");
    target.verify_identical_state()?;
    Ok((measured.0, measured.1, seed_state))
}

fn run_phase_follower(
    target: &mut SqlFollowerTarget,
    config: &Config,
    operations: u64,
    phase: &'static str,
) -> (Samples, Duration) {
    let start = Instant::now();
    let mut samples = Samples::default();
    let mut sequence = 0;
    while sequence < operations {
        let count = config.batch_size.min((operations - sequence) as usize);
        let began = Instant::now();
        let outcome = target.write_sql_qwal_batch(sequence, count, phase, config.value_bytes);
        let batch_elapsed = began.elapsed();
        match outcome {
            Ok(outcome) => {
                samples.record_qwal_phases(outcome.prepare_latency, outcome.apply_latency);
                if let Some(apply) = outcome.follower_apply_latency {
                    samples.record_follower_apply(apply);
                }
                if let Some(bytes) = outcome.envelope_bytes {
                    samples.record_qwal_envelope(bytes);
                }
                samples.record_batch(batch_elapsed, outcome.results);
            }
            Err(error) => {
                samples.record_batch(batch_elapsed, repeated_batch_error(count, error));
            }
        }
        sequence += count as u64;
    }
    (samples, start.elapsed())
}

fn measure_raw(mut target: RawTarget, config: &Config) -> Result<(Samples, Duration), String> {
    if config.profile == Profile::Sql {
        target.prepare_and_apply_sql(&SqlCommand {
            request_id: "profile-bench-schema".into(),
            statements: vec![SqlStatement {
                sql: "CREATE TABLE bench_items(key TEXT PRIMARY KEY, value TEXT NOT NULL)".into(),
                parameters: vec![],
            }],
        })?;
    }
    for index in 0..KEYSPACE {
        let request_id = format!("setup-{index:016x}");
        if config.profile == Profile::Sql {
            target.write_sql_qwal(index, &request_id, config.value_bytes)?;
        } else {
            target.write_one(config.profile, index, &request_id, config.value_bytes)?;
        }
    }
    if config.warmup > 0 {
        let (warmup, _) = run_phase_raw(&mut target, config, config.warmup, "warmup");
        if warmup.errors > 0 {
            return Err(format!("warmup failed with {} errors", warmup.errors));
        }
    }
    Ok(run_phase_raw(
        &mut target,
        config,
        config.operations,
        "measure",
    ))
}

fn run_phase_raw(
    target: &mut RawTarget,
    config: &Config,
    operations: u64,
    phase: &'static str,
) -> (Samples, Duration) {
    let start = Instant::now();
    let mut samples = Samples::default();
    if config.layer == Layer::Qwal && config.workload == Workload::Write {
        let mut sequence = 0;
        while sequence < operations {
            let count = config.batch_size.min((operations - sequence) as usize);
            let began = Instant::now();
            let outcome = target.write_sql_qwal_batch(sequence, count, phase, config.value_bytes);
            let batch_elapsed = began.elapsed();
            match outcome {
                Ok(outcome) => {
                    samples.record_qwal_phases(outcome.prepare_latency, outcome.apply_latency);
                    if let Some(bytes) = outcome.envelope_bytes {
                        samples.record_qwal_envelope(bytes);
                    }
                    samples.record_batch(batch_elapsed, outcome.results);
                }
                Err(error) => {
                    samples.record_batch(batch_elapsed, repeated_batch_error(count, error));
                }
            }
            sequence += count as u64;
        }
    } else {
        for sequence in 0..operations {
            let request_id = format!("{phase}-{sequence:016x}");
            let began = Instant::now();
            let result = target.operate(config, sequence, &request_id);
            if config.workload == Workload::Write {
                samples.record_batch(began.elapsed(), vec![result]);
            } else {
                samples.record(began.elapsed(), result);
            }
        }
    }
    (samples, start.elapsed())
}

struct ConsensusTarget {
    consensus: ThreeNodeConsensus,
    next_index: u64,
    previous_hash: LogHash,
}

impl ConsensusTarget {
    fn open(root: &Path) -> Result<Self, String> {
        let cluster_id = "profile-bench-consensus";
        let membership =
            Membership::new(["node-1", "node-2", "node-3"]).map_err(|error| error.to_string())?;
        let consensus = ThreeNodeConsensus::from_recorders_with_ids(
            cluster_id,
            "node-1",
            1,
            1,
            recorders(root, &membership, cluster_id)?,
        )
        .map_err(|error| error.to_string())?;
        Ok(Self {
            consensus,
            next_index: 1,
            previous_hash: LogHash::ZERO,
        })
    }

    fn write(&mut self, sequence: u64, request_id: &str, bytes: usize) -> Result<(), String> {
        let payload = value(sequence, request_id, bytes).into_bytes();
        let entry = self
            .consensus
            .propose_at(
                RecorderRpcContext::default_timeout(),
                self.next_index,
                self.previous_hash,
                ConsensusCommand::new(CommandKind::Deterministic, payload),
            )
            .map_err(|error| error.to_string())?;
        self.next_index = entry
            .index
            .checked_add(1)
            .ok_or_else(|| "consensus benchmark index exhausted".to_string())?;
        self.previous_hash = entry.hash;
        Ok(())
    }
}

fn measure_consensus(
    mut target: ConsensusTarget,
    config: &Config,
) -> Result<(Samples, Duration), String> {
    if config.warmup > 0 {
        let (warmup, _) = run_phase_consensus(&mut target, config, config.warmup, "warmup");
        if warmup.errors > 0 {
            return Err(format!("warmup failed with {} errors", warmup.errors));
        }
    }
    Ok(run_phase_consensus(
        &mut target,
        config,
        config.operations,
        "measure",
    ))
}

fn run_phase_consensus(
    target: &mut ConsensusTarget,
    config: &Config,
    operations: u64,
    phase: &'static str,
) -> (Samples, Duration) {
    let start = Instant::now();
    let mut samples = Samples::default();
    for sequence in 0..operations {
        let request_id = format!("{phase}-{sequence:016x}");
        let began = Instant::now();
        let result = target.write(sequence, &request_id, config.value_bytes);
        samples.record_batch(began.elapsed(), vec![result]);
    }
    (samples, start.elapsed())
}

fn key(index: u64) -> String {
    format!("bench-key-{index:08}")
}

fn setup_value(index: u64, bytes: usize) -> String {
    value(index, &format!("setup-{index:016x}"), bytes)
}

fn expected_native_keys() -> Vec<String> {
    (0..16).map(key).collect()
}

fn validate_read_tip(profile: Profile, applied_index: u64, hash: LogHash) -> Result<(), String> {
    let minimum_index = KEYSPACE + u64::from(profile == Profile::Sql);
    if applied_index < minimum_index {
        return Err(format!(
            "read observed stale applied index {applied_index}; expected at least {minimum_index}"
        ));
    }
    if hash == LogHash::ZERO {
        return Err("read returned the zero applied hash after seeded writes".into());
    }
    Ok(())
}

fn value(index: u64, request_id: &str, bytes: usize) -> String {
    // Stable FNV-1a keeps the changing portion at the front even for the
    // minimum payload size. The same request/key pair therefore produces the
    // exact same bytes for SQL, graph, and KV without adding a hash dependency.
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in index.to_be_bytes().iter().chain(request_id.as_bytes()) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let pattern = format!("{hash:016x}");
    pattern.chars().cycle().take(bytes).collect()
}

fn provenance() -> Provenance {
    Provenance {
        git_commit: command_output("git", &["rev-parse", "HEAD"]),
        git_dirty: !command_output("git", &["status", "--porcelain"]).is_empty(),
        rustc: command_output("rustc", &["--version"]),
        source: "bench/src/bin/rhiza-profile.rs",
    }
}

fn system() -> System {
    let cpu_model = if cfg!(target_os = "macos") {
        command_output("sysctl", &["-n", "machdep.cpu.brand_string"])
    } else {
        command_output(
            "sh",
            &[
                "-c",
                "sed -n 's/^model name[[:space:]]*: //p' /proc/cpuinfo | head -1",
            ],
        )
    };
    System {
        os: env::consts::OS,
        architecture: env::consts::ARCH,
        logical_cpus: std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1),
        kernel: command_output("uname", &["-srm"]),
        cpu_model,
    }
}

fn command_output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_test_seed_cache(path: &Path, header: Vec<u8>, snapshot: &[u8]) {
        let header_len = u32::try_from(header.len()).unwrap().to_be_bytes();
        let mut file = fs::File::create(path).unwrap();
        file.write_all(SQL_SEED_CACHE_MAGIC).unwrap();
        file.write_all(&header_len).unwrap();
        file.write_all(&header).unwrap();
        file.write_all(snapshot).unwrap();
    }

    #[test]
    fn config_parses_required_and_optional_values() {
        let config = Config::parse(
            [
                "--profile",
                "graph",
                "--workload",
                "native-read",
                "--operations",
                "20",
                "--warmup",
                "0",
                "--concurrency",
                "4",
                "--value-bytes",
                "64",
            ]
            .map(str::to_owned),
        )
        .unwrap();
        assert_eq!(
            config,
            Config {
                layer: Layer::Handle,
                profile: Profile::Graph,
                workload: Workload::NativeRead,
                batch_size: 1,
                operations: 20,
                warmup: 0,
                concurrency: 4,
                value_bytes: 64,
                read_consistency: BenchmarkReadConsistency::Local,
                sql_write_profile: false,
                sql_padding_mib: 0,
                sql_seed_cache: None,
            }
        );
    }

    #[test]
    fn config_selects_runtime_and_rejects_concurrent_raw_measurement() {
        let runtime = Config::parse(
            ["--profile", "kv", "--workload", "get", "--layer", "runtime"].map(str::to_owned),
        )
        .unwrap();
        assert_eq!(runtime.layer, Layer::Runtime);

        assert!(Config::parse(
            [
                "--profile",
                "kv",
                "--workload",
                "get",
                "--layer",
                "raw",
                "--concurrency",
                "2",
            ]
            .map(str::to_owned),
        )
        .is_err());
    }

    #[test]
    fn config_parses_read_consistency_only_for_handle_and_runtime_reads() {
        for layer in ["handle", "runtime"] {
            let barrier = Config::parse(
                [
                    "--profile",
                    "kv",
                    "--workload",
                    "get",
                    "--layer",
                    layer,
                    "--consistency",
                    "read_barrier",
                ]
                .map(str::to_owned),
            )
            .unwrap();
            assert_eq!(
                barrier.read_consistency,
                BenchmarkReadConsistency::ReadBarrier
            );

            let local = Config::parse(
                ["--profile", "kv", "--workload", "get", "--layer", layer].map(str::to_owned),
            )
            .unwrap();
            assert_eq!(local.read_consistency, BenchmarkReadConsistency::Local);
        }

        for invalid in [
            vec![
                "--profile",
                "kv",
                "--workload",
                "write",
                "--consistency",
                "read_barrier",
            ],
            vec![
                "--profile",
                "kv",
                "--workload",
                "get",
                "--layer",
                "raw",
                "--consistency",
                "read_barrier",
            ],
            vec![
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "qwal",
                "--consistency",
                "local",
            ],
            vec![
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "consensus",
                "--consistency",
                "local",
            ],
        ] {
            assert!(Config::parse(invalid.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn config_rejects_unknown_read_consistency() {
        let error = Config::parse(
            [
                "--profile",
                "sql",
                "--workload",
                "get",
                "--consistency",
                "stale",
            ]
            .map(str::to_owned),
        )
        .unwrap_err();

        assert_eq!(error, "--consistency must be local or read_barrier");
    }

    #[test]
    fn sql_write_profiler_is_opt_in_only_for_runtime_sql_writes() {
        let enabled = Config::parse(
            [
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "runtime",
                "--sql-write-profile",
            ]
            .map(str::to_owned),
        )
        .unwrap();
        assert!(enabled.sql_write_profile);

        for invalid in [
            vec![
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "handle",
                "--sql-write-profile",
            ],
            vec![
                "--profile",
                "kv",
                "--workload",
                "write",
                "--layer",
                "runtime",
                "--sql-write-profile",
            ],
            vec![
                "--profile",
                "sql",
                "--workload",
                "get",
                "--layer",
                "runtime",
                "--sql-write-profile",
            ],
        ] {
            assert!(Config::parse(invalid.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn consensus_layer_accepts_only_single_worker_writes() {
        let consensus = Config::parse(
            [
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "consensus",
            ]
            .map(str::to_owned),
        )
        .unwrap();
        assert_eq!(consensus.layer, Layer::Consensus);

        for invalid in [
            vec![
                "--profile",
                "sql",
                "--workload",
                "get",
                "--layer",
                "consensus",
            ],
            vec![
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "consensus",
                "--concurrency",
                "2",
            ],
        ] {
            assert!(Config::parse(invalid.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn document_get_accepts_only_graph_profile() {
        for layer in ["handle", "runtime", "raw"] {
            let config = Config::parse(
                [
                    "--profile",
                    "graph",
                    "--workload",
                    "document-get",
                    "--layer",
                    layer,
                ]
                .map(str::to_owned),
            )
            .unwrap();
            assert_eq!(config.workload, Workload::DocumentGet);
        }

        for profile in ["sql", "kv"] {
            assert!(Config::parse(
                ["--profile", profile, "--workload", "document-get"].map(str::to_owned)
            )
            .is_err());
        }
    }

    #[test]
    fn raw_document_get_reads_seeded_graph_document() {
        let root = tempfile::tempdir().unwrap();
        let mut target = RawTarget::open(root.path(), Profile::Graph).unwrap();
        target
            .write_one(Profile::Graph, 7, "setup-graph-document", 64)
            .unwrap();

        target.get_graph_document(Profile::Graph, 7).unwrap();
    }

    #[test]
    fn raw_sql_write_rejects_invalid_qsql_apply_measurement() {
        let error = Config::parse(
            ["--profile", "sql", "--workload", "write", "--layer", "raw"].map(str::to_owned),
        )
        .unwrap_err();

        assert_eq!(
            error,
            "--layer raw cannot measure SQL writes: SQLite apply accepts only prepared QWAL v3 effects; use --layer qwal"
        );
    }

    #[test]
    fn qwal_layer_accepts_only_single_worker_sql_writes() {
        let qwal = Config::parse(
            ["--profile", "sql", "--workload", "write", "--layer", "qwal"].map(str::to_owned),
        )
        .unwrap();
        assert_eq!(qwal.layer, Layer::Qwal);
        let batched = Config::parse(
            [
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "qwal",
                "--batch-size",
                "256",
            ]
            .map(str::to_owned),
        )
        .unwrap();
        assert_eq!(batched.batch_size, 256);

        for invalid in [
            vec!["--profile", "kv", "--workload", "write", "--layer", "qwal"],
            vec!["--profile", "sql", "--workload", "get", "--layer", "qwal"],
            vec![
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "qwal",
                "--concurrency",
                "2",
            ],
        ] {
            assert!(Config::parse(invalid.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn follower_apply_accepts_only_single_worker_sql_writes_and_bounded_padding() {
        for padding in ["0", "1024"] {
            let config = Config::parse(
                [
                    "--profile",
                    "sql",
                    "--workload",
                    "write",
                    "--layer",
                    "follower-apply",
                    "--sql-padding-mib",
                    padding,
                ]
                .map(str::to_owned),
            )
            .unwrap();
            assert_eq!(config.layer, Layer::FollowerApply);
            assert_eq!(config.sql_padding_mib.to_string(), padding);
        }

        for invalid in [
            vec![
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "follower-apply",
                "--sql-padding-mib",
                "1025",
            ],
            vec![
                "--profile",
                "kv",
                "--workload",
                "write",
                "--layer",
                "follower-apply",
            ],
            vec![
                "--profile",
                "sql",
                "--workload",
                "get",
                "--layer",
                "follower-apply",
            ],
            vec![
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "follower-apply",
                "--concurrency",
                "2",
            ],
            vec![
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "qwal",
                "--sql-padding-mib",
                "1",
            ],
        ] {
            assert!(Config::parse(invalid.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn follower_seed_cache_is_explicit_and_follower_apply_only() {
        let cache = tempfile::tempdir().unwrap().path().join("seed.cache");
        let cache_text = cache.to_str().unwrap();
        let config = Config::parse(
            [
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "follower-apply",
                "--sql-seed-cache",
                cache_text,
            ]
            .map(str::to_owned),
        )
        .unwrap();
        assert_eq!(config.sql_seed_cache, Some(cache.clone()));

        assert!(Config::parse(
            [
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "qwal",
                "--sql-seed-cache",
                cache_text,
            ]
            .map(str::to_owned),
        )
        .is_err());
    }

    #[test]
    fn follower_seed_cache_miss_then_hit_restores_the_same_seed_identity() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("cache/sql-padding-0.cache");
        let first_root = tempfile::tempdir().unwrap();
        let (_, created) = SqlFollowerTarget::seed(first_root.path(), 64, 0, Some(&cache)).unwrap();

        let second_root = tempfile::tempdir().unwrap();
        let (_, hit) = SqlFollowerTarget::seed(second_root.path(), 64, 0, Some(&cache)).unwrap();

        let created_cache = created.seed_cache.as_ref().unwrap();
        let hit_cache = hit.seed_cache.as_ref().unwrap();
        assert_eq!(created_cache.disposition, "created");
        assert_eq!(hit_cache.disposition, "hit");
        assert_eq!(created_cache.path, hit_cache.path);
        assert_eq!(created_cache.digest, hit_cache.digest);
        assert_eq!(created_cache.snapshot_digest, hit_cache.snapshot_digest);
        assert_eq!(created_cache.bytes, hit_cache.bytes);
        assert_eq!(created.receipt_count, hit.receipt_count);
        assert_eq!(created.leader_database_bytes, hit.leader_database_bytes);
        assert_eq!(created.leader_control_bytes, hit.leader_control_bytes);
        assert!(first_root.path().join("raw/leader-seed.db").is_file());
        assert!(!first_root.path().join("raw/follower-seed.db").exists());
        assert!(!second_root.path().join("raw/leader-seed.db").exists());
        assert!(!second_root.path().join("raw/follower-seed.db").exists());
    }

    #[test]
    fn follower_seed_cache_rejects_tampering_and_wrong_seed_configuration() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("seed.cache");
        let seed_root = tempfile::tempdir().unwrap();
        SqlFollowerTarget::seed(seed_root.path(), 64, 0, Some(&cache)).unwrap();

        let wrong_padding_root = tempfile::tempdir().unwrap();
        assert!(SqlFollowerTarget::seed(wrong_padding_root.path(), 64, 1, Some(&cache)).is_err());
        let wrong_value_root = tempfile::tempdir().unwrap();
        assert!(SqlFollowerTarget::seed(wrong_value_root.path(), 65, 0, Some(&cache)).is_err());

        let mut bytes = std::fs::read(&cache).unwrap();
        let last = bytes.last_mut().unwrap();
        *last ^= 0xff;
        std::fs::write(&cache, bytes).unwrap();
        let tampered_root = tempfile::tempdir().unwrap();
        assert!(SqlFollowerTarget::seed(tampered_root.path(), 64, 0, Some(&cache)).is_err());
    }

    #[test]
    fn invalid_generated_seed_is_validated_before_cache_publication() {
        let source_root = tempfile::tempdir().unwrap();
        let source_cache = source_root.path().join("source.seed");
        SqlFollowerTarget::seed(source_root.path(), 64, 0, Some(&source_cache)).unwrap();
        let loaded = load_sql_seed_cache(
            &source_cache,
            0,
            64,
            expected_seed_receipt_count(0).unwrap(),
        )
        .unwrap();
        let mut invalid_bytes = loaded.snapshot.db_bytes().to_vec();
        invalid_bytes[0] ^= 0xff;
        let invalid = Snapshot::new(loaded.snapshot.manifest().clone(), invalid_bytes);
        let restore_root = tempfile::tempdir().unwrap();
        let cache = restore_root.path().join("must-not-exist.seed");

        assert!(SqlFollowerTarget::restore_validated_seed(
            restore_root.path(),
            &invalid,
            64,
            0,
            expected_seed_receipt_count(0).unwrap(),
            Some(&cache),
        )
        .is_err());
        assert!(!cache.exists());
    }

    #[test]
    fn follower_seed_cache_rejects_unsafe_files_before_snapshot_allocation() {
        let root = tempfile::tempdir().unwrap();
        let receipt_count = expected_seed_receipt_count(0).unwrap();

        let huge = root.path().join("huge.seed");
        let huge_file = fs::File::create(&huge).unwrap();
        huge_file
            .set_len(sql_seed_cache_max_file_bytes(0, 64).unwrap() + 1)
            .unwrap();
        drop(huge_file);
        assert!(load_sql_seed_cache(&huge, 0, 64, receipt_count).is_err());

        let directory = root.path().join("directory.seed");
        fs::create_dir(&directory).unwrap();
        assert!(load_sql_seed_cache(&directory, 0, 64, receipt_count).is_err());

        let truncated = root.path().join("truncated.seed");
        fs::write(&truncated, SQL_SEED_CACHE_MAGIC).unwrap();
        assert!(load_sql_seed_cache(&truncated, 0, 64, receipt_count).is_err());

        #[cfg(unix)]
        {
            let target = root.path().join("target.seed");
            fs::write(&target, b"not a cache").unwrap();
            let link = root.path().join("link.seed");
            std::os::unix::fs::symlink(&target, &link).unwrap();
            assert!(load_sql_seed_cache(&link, 0, 64, receipt_count).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn opened_file_seal_rejects_same_inode_same_length_rewrite() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("same-length.seed");
        fs::write(&path, b"before").unwrap();
        let opened = fs::File::open(&path).unwrap();
        let before = opened.metadata().unwrap();

        fs::write(&path, b"after!").unwrap();
        fs::File::open(&path)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(1)))
            .unwrap();
        let after = opened.metadata().unwrap();

        assert_eq!(before.len(), after.len());
        assert!(!same_opened_file(&before, &after));
    }

    #[test]
    fn follower_seed_cache_rejects_noncanonical_and_wrong_format_headers() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("valid.seed");
        let seed_root = tempfile::tempdir().unwrap();
        SqlFollowerTarget::seed(seed_root.path(), 64, 0, Some(&cache)).unwrap();
        let encoded = fs::read(&cache).unwrap();
        let length_offset = SQL_SEED_CACHE_MAGIC.len();
        let header_len = u32::from_be_bytes(
            encoded[length_offset..length_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let header_start = length_offset + 4;
        let header_end = header_start + header_len;
        let header: SqlSeedCacheHeader =
            serde_json::from_slice(&encoded[header_start..header_end]).unwrap();

        let noncanonical = root.path().join("noncanonical.seed");
        write_test_seed_cache(
            &noncanonical,
            [serde_json::to_vec(&header).unwrap(), b"\n".to_vec()].concat(),
            &encoded[header_end..],
        );
        assert!(load_sql_seed_cache(
            &noncanonical,
            0,
            64,
            expected_seed_receipt_count(0).unwrap(),
        )
        .is_err());

        let mut wrong_format = header.clone();
        wrong_format.format_version += 1;
        let mut wrong_recipe = header.clone();
        wrong_recipe.seed_recipe_id.push_str("-wrong");
        let mut wrong_configuration = header;
        wrong_configuration.configuration_state = ConfigurationState::active(2, LogHash::ZERO);
        for (name, wrong_header) in [
            ("wrong-format.seed", wrong_format),
            ("wrong-recipe.seed", wrong_recipe),
            ("wrong-configuration.seed", wrong_configuration),
        ] {
            let path = root.path().join(name);
            write_test_seed_cache(
                &path,
                serde_json::to_vec(&wrong_header).unwrap(),
                &encoded[header_end..],
            );
            assert!(
                load_sql_seed_cache(&path, 0, 64, expected_seed_receipt_count(0).unwrap(),)
                    .is_err()
            );
        }
    }

    #[test]
    fn follower_seed_cache_never_clobbers_an_existing_path() {
        let root = tempfile::tempdir().unwrap();
        let cache = root.path().join("occupied.seed");
        let original = b"keep this exact file";
        fs::write(&cache, original).unwrap();
        let seed_root = tempfile::tempdir().unwrap();

        assert!(SqlFollowerTarget::seed(seed_root.path(), 64, 0, Some(&cache)).is_err());
        assert_eq!(fs::read(cache).unwrap(), original);
    }

    #[test]
    fn padding_rows_require_exact_ids_lengths_and_zero_content() {
        let chunk_count = 1024 * 1024 / SQL_PADDING_CHUNK_BYTES;
        let valid = (0..chunk_count)
            .map(|index| {
                vec![
                    SqlValue::Integer(index as i64),
                    SqlValue::Integer(SQL_PADDING_CHUNK_BYTES as i64),
                    SqlValue::Integer(1),
                ]
            })
            .collect::<Vec<_>>();
        assert!(validate_padding_rows(&valid, 1).is_ok());

        for invalid in [
            {
                let mut rows = valid.clone();
                rows[2][0] = SqlValue::Integer(3);
                rows
            },
            {
                let mut rows = valid.clone();
                rows[2][1] = SqlValue::Integer((SQL_PADDING_CHUNK_BYTES - 1) as i64);
                rows
            },
            {
                let mut rows = valid.clone();
                rows[2][2] = SqlValue::Integer(0);
                rows
            },
            valid[..valid.len() - 1].to_vec(),
        ] {
            assert!(validate_padding_rows(&invalid, 1).is_err());
        }
    }

    #[test]
    fn seed_recipe_digest_binds_order_padding_and_values() {
        let digest = sql_seed_recipe_digest(64, 0).unwrap();
        assert_eq!(digest, sql_seed_recipe_digest(64, 0).unwrap());
        assert_ne!(digest, sql_seed_recipe_digest(65, 0).unwrap());
        assert_ne!(digest, sql_seed_recipe_digest(64, 1).unwrap());
    }

    #[test]
    fn follower_apply_seed_restores_identical_compacted_states_with_exact_padding() {
        let root = tempfile::tempdir().unwrap();
        let (target, seed) = SqlFollowerTarget::seed(root.path(), 64, 1, None).unwrap();

        assert_eq!(seed.receipt_count, KEYSPACE + 9);
        assert_eq!(seed.leader_embedded_qlog_entries, 0);
        assert_eq!(seed.follower_embedded_qlog_entries, 0);
        assert!(seed.leader_database_bytes >= 1024 * 1024);
        assert_eq!(seed.leader_database_bytes, seed.follower_database_bytes);
        assert!(seed.leader_control_bytes > 0);
        assert_eq!(seed.leader_control_bytes, seed.follower_control_bytes);

        assert_eq!(
            target
                .leader
                .sql_state()
                .unwrap()
                .canonical_db_digest()
                .unwrap(),
            target
                .follower
                .sql_state()
                .unwrap()
                .canonical_db_digest()
                .unwrap()
        );
        assert_eq!(
            target
                .leader
                .sql_state()
                .unwrap()
                .applied_tip_value()
                .unwrap(),
            target
                .follower
                .sql_state()
                .unwrap()
                .applied_tip_value()
                .unwrap()
        );
        assert_eq!(
            sql_scalar_integer(
                target.follower.sql_state().unwrap(),
                "SELECT COALESCE(sum(length(payload)), 0) FROM bench_padding",
            ),
            1024 * 1024
        );
    }

    #[test]
    fn follower_apply_replays_the_prebuilt_leader_entry_and_reports_apply_latency() {
        let root = tempfile::tempdir().unwrap();
        let (mut target, _) = SqlFollowerTarget::seed(root.path(), 64, 0, None).unwrap();

        let outcome = target
            .write_sql_qwal_batch(7, 1, "follower-test", 64)
            .unwrap();

        assert!(outcome.results.iter().all(Result::is_ok));
        assert!(outcome.follower_apply_latency.is_some());
        assert!(outcome.apply_latency.is_none());
        assert_eq!(
            sql_scalar_text(
                target.follower.sql_state().unwrap(),
                "SELECT value FROM bench_items WHERE key = 'bench-key-00000007'",
            ),
            value(7, "follower-test-0000000000000007", 64)
        );
        assert_eq!(
            target
                .leader
                .sql_state()
                .unwrap()
                .canonical_db_digest()
                .unwrap(),
            target
                .follower
                .sql_state()
                .unwrap()
                .canonical_db_digest()
                .unwrap()
        );
    }

    #[test]
    fn follower_hot_row_effect_excludes_untouched_padding_root_page() {
        let root = tempfile::tempdir().unwrap();
        let (mut target, _) = SqlFollowerTarget::seed(root.path(), 64, 1, None).unwrap();
        let padding_root = sql_scalar_integer(
            target.leader.sql_state().unwrap(),
            "SELECT rootpage FROM sqlite_schema WHERE name = 'bench_padding'",
        ) as u32;

        let prepared = target
            .leader
            .prepare_sql_batch(&[sql_write_command(9, "padding-audit", 64)])
            .unwrap();
        let effect = decode_qwal_v3(prepared.payload.as_ref().unwrap()).unwrap();

        assert!(effect.pages.iter().all(|page| page.page_no != padding_root));
    }

    fn sql_scalar_integer(state: &SqliteStateMachine, sql: &str) -> i64 {
        let result = state
            .query_sql(
                &SqlStatement {
                    sql: sql.into(),
                    parameters: vec![],
                },
                1,
                RAW_RESULT_BYTES,
            )
            .unwrap();
        let SqlValue::Integer(value) = result.rows[0][0] else {
            panic!("expected one integer SQL value")
        };
        value
    }

    fn sql_scalar_text(state: &SqliteStateMachine, sql: &str) -> String {
        let result = state
            .query_sql(
                &SqlStatement {
                    sql: sql.into(),
                    parameters: vec![],
                },
                1,
                RAW_RESULT_BYTES,
            )
            .unwrap();
        let SqlValue::Text(value) = &result.rows[0][0] else {
            panic!("expected one text SQL value")
        };
        value.clone()
    }

    #[test]
    fn qwal_sql_write_is_queryable_after_preparation_and_apply() {
        let root = tempfile::tempdir().unwrap();
        let mut target = RawTarget::open(root.path(), Profile::Sql).unwrap();
        target
            .prepare_and_apply_sql(&SqlCommand {
                request_id: "test-schema".into(),
                statements: vec![SqlStatement {
                    sql: "CREATE TABLE bench_items(key TEXT PRIMARY KEY, value TEXT NOT NULL)"
                        .into(),
                    parameters: vec![],
                }],
            })
            .unwrap();
        let index_before_batch = target.next_index;
        let outcome = target.write_sql_qwal_batch(7, 2, "test-write", 64).unwrap();

        target.get_one(Profile::Sql, 7).unwrap();
        target.get_one(Profile::Sql, 8).unwrap();
        assert!(outcome.results.iter().all(Result::is_ok));
        assert!(outcome
            .envelope_bytes
            .is_some_and(|bytes| bytes > QWAL_V3_MAGIC.len()));
        assert_eq!(target.next_index, index_before_batch + 1);
    }

    #[test]
    fn config_rejects_zero_operations_and_unbounded_values() {
        assert!(Config::parse(
            ["--profile", "sql", "--workload", "get", "--operations", "0"].map(str::to_owned)
        )
        .is_err());
        assert!(Config::parse(
            [
                "--profile",
                "kv",
                "--workload",
                "write",
                "--value-bytes",
                "4097",
            ]
            .map(str::to_owned)
        )
        .is_err());
    }

    #[test]
    fn write_batch_size_matches_profile_and_layer_caps() {
        for layer in ["handle", "runtime"] {
            let graph_error = Config::parse(
                [
                    "--layer",
                    layer,
                    "--profile",
                    "graph",
                    "--workload",
                    "write",
                    "--batch-size",
                    "128",
                ]
                .map(str::to_owned),
            )
            .unwrap_err();
            assert_eq!(
                graph_error,
                format!("--profile graph supports --batch-size at most 64 on --layer {layer}")
            );

            for profile in ["kv", "sql"] {
                let config = Config::parse(
                    [
                        "--layer",
                        layer,
                        "--profile",
                        profile,
                        "--workload",
                        "write",
                        "--batch-size",
                        "256",
                    ]
                    .map(str::to_owned),
                )
                .unwrap();
                assert_eq!(config.batch_size, 256);
            }
        }

        for batch_size in [512, 1024] {
            let config = Config::parse(
                [
                    "--layer",
                    "qwal",
                    "--profile",
                    "sql",
                    "--workload",
                    "write",
                    "--batch-size",
                    &batch_size.to_string(),
                ]
                .map(str::to_owned),
            )
            .unwrap();
            assert_eq!(config.batch_size, batch_size);

            assert!(Config::parse(
                [
                    "--layer",
                    "runtime",
                    "--profile",
                    "sql",
                    "--workload",
                    "write",
                    "--batch-size",
                    &batch_size.to_string(),
                ]
                .map(str::to_owned),
            )
            .is_err());
        }
        for args in [
            vec![
                "--profile",
                "sql",
                "--workload",
                "write",
                "--batch-size",
                "3",
            ],
            vec!["--profile", "sql", "--workload", "get", "--batch-size", "2"],
            vec![
                "--profile",
                "sql",
                "--workload",
                "write",
                "--layer",
                "raw",
                "--batch-size",
                "2",
            ],
        ] {
            assert!(Config::parse(args.into_iter().map(str::to_owned)).is_err());
        }
    }

    #[test]
    fn percentiles_use_nearest_rank_and_merge_worker_histograms() {
        let mut left = Samples::default();
        left.record(Duration::from_micros(10), Ok(()));
        left.record(Duration::from_micros(20), Ok(()));
        let mut right = Samples::default();
        right.record(Duration::from_micros(30), Ok(()));
        right.record(Duration::from_micros(40), Ok(()));
        right.record(Duration::from_micros(50), Err("failed".into()));
        left.merge(right);

        assert_eq!(left.successes, 4);
        assert_eq!(left.errors, 1);
        assert_eq!(left.percentile(500), Some(20));
        assert_eq!(left.percentile(950), Some(40));
        assert_eq!(
            left.metrics(Duration::from_secs(2), None)
                .operations_per_second,
            2.0
        );
    }

    #[test]
    fn batch_metrics_separate_call_latency_from_amortized_item_latency() {
        let mut samples = Samples::default();
        samples.record_batch(
            Duration::from_micros(120),
            vec![Ok(()), Ok(()), Ok(()), Ok(())],
        );
        samples.record_qwal_envelope(4_096);
        samples.record_qwal_phases(Duration::from_micros(80), Some(Duration::from_micros(30)));

        let metrics = samples.metrics(Duration::from_secs(1), Some(1));
        assert_eq!(metrics.batch_calls, Some(1));
        assert_eq!(metrics.successful_batch_calls, Some(1));
        assert_eq!(metrics.failed_batch_calls, Some(0));
        assert_eq!(metrics.batch_call_latency_us.unwrap().p50, Some(120));
        assert_eq!(metrics.logical_item_latency_us.p50, Some(30));
        assert_eq!(metrics.logical_operations_per_qlog, Some(4.0));
        assert_eq!(metrics.qwal_prepare_latency_us.unwrap().p50, Some(80));
        assert_eq!(metrics.qwal_apply_latency_us.unwrap().p50, Some(30));
        assert_eq!(
            metrics.qwal_envelope_bytes,
            Some(QwalEnvelopeBytes {
                count: 1,
                total: 4_096,
                average: 4_096.0,
                min: 4_096,
                max: 4_096,
            })
        );
    }

    #[test]
    fn follower_apply_metrics_are_separate_from_leader_prepare_and_apply() {
        let mut samples = Samples::default();
        samples.record_qwal_phases(Duration::from_micros(80), None);
        samples.record_follower_apply(Duration::from_micros(25));

        let metrics = samples.metrics(Duration::from_secs(1), None);

        assert_eq!(metrics.qwal_prepare_latency_us.unwrap().p50, Some(80));
        assert!(metrics.qwal_apply_latency_us.is_none());
        assert_eq!(metrics.follower_apply_latency_us.unwrap().p50, Some(25));
    }

    #[test]
    fn schema_v6_sql_profile_reports_validated_phase_histograms() {
        let mut samples = SqlWritePhaseSamples::default();
        samples
            .record_snapshot(
                SqlWriteProfileSnapshot {
                    samples: vec![sql_write_profile_sample()],
                    dropped_samples: 0,
                },
                1,
                4,
            )
            .unwrap();

        let metrics = samples.metrics().unwrap();
        assert_eq!(REPORT_SCHEMA_VERSION, 6);
        assert_eq!(metrics.sample_count, 1);
        assert_eq!(metrics.member_count, 4);
        assert_eq!(metrics.dropped_samples, 0);
        assert_eq!(metrics.phase_latency_us.commit_lock_wait.total_us, 10);
        assert_eq!(
            metrics.phase_latency_us.qwal_prepare.latency_us.p50,
            Some(30)
        );
        assert_eq!(metrics.phase_latency_us.total_service.total_us, 280);
        let json = serde_json::to_value(metrics).unwrap();
        assert_eq!(
            json["phase_latency_us"]["consensus_propose"]["total_us"],
            40
        );
        assert_eq!(
            json["phase_latency_us"]["local_qlog_mirror_append"]["latency_us"]["p95"],
            50
        );
    }

    #[test]
    fn sql_profile_rejects_sample_member_and_phase_sum_mismatches() {
        let snapshot = SqlWriteProfileSnapshot {
            samples: vec![sql_write_profile_sample()],
            dropped_samples: 0,
        };
        assert!(SqlWritePhaseSamples::default()
            .record_snapshot(snapshot.clone(), 2, 4)
            .is_err());
        assert!(SqlWritePhaseSamples::default()
            .record_snapshot(snapshot.clone(), 1, 3)
            .is_err());

        let mut invalid = sql_write_profile_sample();
        invalid.total_service_us += 1;
        assert!(SqlWritePhaseSamples::default()
            .record_snapshot(
                SqlWriteProfileSnapshot {
                    samples: vec![invalid],
                    dropped_samples: 0,
                },
                1,
                4,
            )
            .is_err());
    }

    fn sql_write_profile_sample() -> rhiza_node::SqlWriteProfileSample {
        rhiza_node::SqlWriteProfileSample {
            batch_member_count: 4,
            commit_lock_wait_us: 10,
            precheck_classification_us: 20,
            qwal_prepare_us: 30,
            consensus_propose_us: 40,
            local_qlog_mirror_append_us: 50,
            sql_materializer_apply_us: 60,
            response_other_total_us: 70,
            total_service_us: 280,
        }
    }

    #[test]
    fn report_latency_keys_are_stable() {
        let json = serde_json::to_value(Latencies {
            p50: Some(1),
            p95: Some(2),
            p99: Some(3),
            p99_9: Some(4),
            max: Some(5),
        })
        .unwrap();
        assert_eq!(json["p99.9"], 4);
        assert_eq!(json["max"], 5);
    }

    #[test]
    fn measured_write_values_change_without_changing_payload_size() {
        let first = value(7, "measure-0000000000000001", 128);
        let second = value(7, "measure-0000000000000002", 128);

        assert_eq!(first.len(), 128);
        assert_eq!(second.len(), 128);
        assert_ne!(first, second);
        assert_eq!(first, value(7, "measure-0000000000000001", 128));
    }
}
