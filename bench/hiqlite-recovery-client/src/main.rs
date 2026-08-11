use hiqlite::{Client, Params, Row};
use serde_json::{Value, json};
use std::env;
use std::error::Error;
use std::fmt::{self, Display};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};

#[derive(Debug)]
struct CliError(String);

impl Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for CliError {}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Sentinel {
    id: String,
    value: String,
}

struct RowCount(u64);

impl From<&mut Row<'_>> for RowCount {
    fn from(row: &mut Row<'_>) -> Self {
        let count: i64 = row.get("count");
        Self(count.try_into().expect("COUNT(*) cannot be negative"))
    }
}

struct ValueLedger {
    count: u64,
    min: Option<String>,
    max: Option<String>,
}
impl From<&mut Row<'_>> for ValueLedger {
    fn from(row: &mut Row<'_>) -> Self {
        let count: i64 = row.get("count");
        Self {
            count: count.try_into().expect("COUNT(*) cannot be negative"),
            min: row.get("min_value"),
            max: row.get("max_value"),
        }
    }
}

impl From<&mut Row<'_>> for Sentinel {
    fn from(row: &mut Row<'_>) -> Self {
        Self {
            id: row.get("id"),
            value: row.get("value"),
        }
    }
}

struct Args {
    nodes: Vec<String>,
    secret: String,
    command: String,
    command_args: Vec<String>,
}

fn usage() -> &'static str {
    "usage: hiqlite-recovery-client --nodes host:port[,host:port] --secret SECRET \
<execute ID VALUE|reset|query-local ID|query-consistent ID|backup|metrics|verify-sentinel ID VALUE|bench-write --warmup-seconds N --measure-seconds N --concurrency N --id ID --value VALUE>"
}

fn parse_args() -> Result<Args, CliError> {
    let mut args = env::args().skip(1);
    let mut nodes = None;
    let mut secret = None;
    let mut command = None;
    let mut command_args = Vec::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--nodes" => {
                let value = args
                    .next()
                    .ok_or_else(|| CliError("--nodes requires a value".into()))?;
                nodes = Some(
                    value
                        .split(',')
                        .filter(|item| !item.is_empty())
                        .map(ToOwned::to_owned)
                        .collect::<Vec<_>>(),
                );
            }
            "--secret" => {
                secret = Some(
                    args.next()
                        .ok_or_else(|| CliError("--secret requires a value".into()))?,
                );
            }
            "-h" | "--help" => return Err(CliError(usage().into())),
            value if command.is_none() => command = Some(value.to_owned()),
            value => command_args.push(value.to_owned()),
        }
    }

    let nodes = nodes.ok_or_else(|| CliError(format!("missing --nodes\n{}", usage())))?;
    if nodes.is_empty() {
        return Err(CliError("--nodes must contain at least one address".into()));
    }

    Ok(Args {
        nodes,
        secret: secret.ok_or_else(|| CliError(format!("missing --secret\n{}", usage())))?,
        command: command.ok_or_else(|| CliError(format!("missing command\n{}", usage())))?,
        command_args,
    })
}

fn require_command_args(args: &[String], count: usize, command: &str) -> Result<(), CliError> {
    if args.len() == count {
        Ok(())
    } else {
        Err(CliError(format!(
            "{command} expects {count} arguments, got {}",
            args.len()
        )))
    }
}

fn params(values: impl IntoIterator<Item = hiqlite::Param>) -> Params {
    values.into_iter().collect()
}

async fn ensure_schema(client: &Client) -> Result<(), hiqlite::Error> {
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS hiqlite_recovery_sentinel (\
             id TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)",
            Params::new(),
        )
        .await?;
    Ok(())
}

async fn query_local(client: &Client, id: &str) -> Result<Option<Sentinel>, hiqlite::Error> {
    let mut rows: Vec<Sentinel> = client
        .query_map(
            "SELECT id, value FROM hiqlite_recovery_sentinel WHERE id = $1",
            params([id.into()]),
        )
        .await?;
    Ok(rows.pop())
}

async fn query_consistent(client: &Client, id: &str) -> Result<Option<Sentinel>, hiqlite::Error> {
    let mut rows: Vec<Sentinel> = client
        .query_consistent_map(
            "SELECT id, value FROM hiqlite_recovery_sentinel WHERE id = $1",
            params([id.into()]),
        )
        .await?;
    Ok(rows.pop())
}

async fn count_consistent(client: &Client, id_prefix: &str) -> Result<u64, hiqlite::Error> {
    let rows: Vec<RowCount> = client
        .query_consistent_map(
            "SELECT COUNT(*) AS count FROM hiqlite_recovery_sentinel WHERE id LIKE $1",
            params([format!("{id_prefix}%").into()]),
        )
        .await?;
    Ok(rows.into_iter().next().map_or(0, |row| row.0))
}

async fn value_ledger_consistent(
    client: &Client,
    id_prefix: &str,
) -> Result<ValueLedger, hiqlite::Error> {
    let rows: Vec<ValueLedger> = client.query_consistent_map(
        "SELECT COUNT(*) AS count, MIN(value) AS min_value, MAX(value) AS max_value FROM hiqlite_recovery_sentinel WHERE id LIKE $1",
        params([format!("{id_prefix}%").into()]),
    ).await?;
    Ok(rows.into_iter().next().unwrap_or(ValueLedger {
        count: 0,
        min: None,
        max: None,
    }))
}

fn sentinel_json(mode: &str, id: &str, sentinel: Option<Sentinel>) -> Value {
    match sentinel {
        Some(value) => json!({
            "command": mode,
            "found": true,
            "id": value.id,
            "value": value.value,
        }),
        None => json!({
            "command": mode,
            "found": false,
            "id": id,
        }),
    }
}

fn print_json(value: Value) -> Result<(), Box<dyn Error>> {
    println!("{}", serde_json::to_string(&value)?);
    Ok(())
}

struct BenchArgs {
    warmup: Duration,
    measure: Duration,
    concurrency: usize,
    id: String,
    value: String,
}

fn bench_args(args: &[String]) -> Result<BenchArgs, CliError> {
    let mut warmup = None;
    let mut measure = None;
    let mut concurrency = None;
    let mut id = None;
    let mut value = None;
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        index += 1;
        let next = || {
            args.get(index)
                .ok_or_else(|| CliError(format!("{flag} requires a value")))
        };
        match flag.as_str() {
            "--warmup-seconds" => {
                warmup = Some(
                    next()?
                        .parse::<u64>()
                        .map_err(|_| CliError("--warmup-seconds must be an integer".into()))?,
                )
            }
            "--measure-seconds" => {
                measure = Some(
                    next()?
                        .parse::<u64>()
                        .map_err(|_| CliError("--measure-seconds must be an integer".into()))?,
                )
            }
            "--concurrency" => {
                concurrency = Some(
                    next()?
                        .parse::<usize>()
                        .map_err(|_| CliError("--concurrency must be an integer".into()))?,
                )
            }
            "--id" => id = Some(next()?.clone()),
            "--value" => value = Some(next()?.clone()),
            _ => return Err(CliError(format!("unknown bench-write option: {flag}"))),
        }
        index += 1;
    }
    let (warmup, measure, concurrency, id, value) = (
        warmup.ok_or_else(|| CliError("bench-write requires --warmup-seconds".into()))?,
        measure.ok_or_else(|| CliError("bench-write requires --measure-seconds".into()))?,
        concurrency.ok_or_else(|| CliError("bench-write requires --concurrency".into()))?,
        id.ok_or_else(|| CliError("bench-write requires --id".into()))?,
        value.ok_or_else(|| CliError("bench-write requires --value".into()))?,
    );
    if warmup == 0
        || measure == 0
        || concurrency == 0
        || id.is_empty()
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        || !value.is_ascii()
        || value.len() != 128
    {
        return Err(CliError("bench-write requires positive durations/concurrency, nonempty id, and exactly 128-byte value".into()));
    }
    Ok(BenchArgs {
        warmup: Duration::from_secs(warmup),
        measure: Duration::from_secs(measure),
        concurrency,
        id,
        value,
    })
}

#[derive(Default)]
struct BenchStats {
    attempts: u64,
    successes: u64,
    errors: u64,
    latencies_ns: Vec<u128>,
}

async fn closed_loop(
    client: Arc<Client>,
    id_prefix: String,
    value_salt: String,
    namespace: &'static str,
    sequence: Arc<AtomicU64>,
    duration: Duration,
) -> BenchStats {
    let deadline = Instant::now() + duration;
    let mut stats = BenchStats::default();
    while Instant::now() < deadline {
        stats.attempts += 1;
        let request_sequence = sequence.fetch_add(1, Ordering::Relaxed);
        let id = format!("{id_prefix}-{namespace}-{request_sequence:020}");
        let value = bench_value(namespace, request_sequence, &value_salt);
        let started = Instant::now();
        match client
            .execute(
                "INSERT INTO hiqlite_recovery_sentinel (id, value) VALUES ($1, $2)",
                params([id.as_str().into(), value.as_str().into()]),
            )
            .await
        {
            Ok(_) => {
                stats.successes += 1;
                stats.latencies_ns.push(started.elapsed().as_nanos());
            }
            Err(_) => stats.errors += 1,
        }
    }
    stats
}

fn percentile_ns(latencies: &mut [u128], numerator: usize, denominator: usize) -> Option<u128> {
    if latencies.is_empty() {
        return None;
    }
    latencies.sort_unstable();
    let index = ((latencies.len() * numerator).div_ceil(denominator)).saturating_sub(1);
    latencies.get(index).copied()
}

fn bench_value(_namespace: &str, _sequence: u64, value_salt: &str) -> String {
    value_salt.to_owned()
}

async fn run_bench(client: &Client, args: BenchArgs) -> Result<Value, Box<dyn Error>> {
    ensure_schema(client).await?;
    let shared = Arc::new(client.clone());
    let sequence = Arc::new(AtomicU64::new(0));
    let mut warmup_workers = Vec::with_capacity(args.concurrency);
    for _ in 0..args.concurrency {
        warmup_workers.push(tokio::spawn(closed_loop(
            shared.clone(),
            args.id.clone(),
            args.value.clone(),
            "warmup",
            sequence.clone(),
            args.warmup,
        )));
    }
    let mut warmup = BenchStats::default();
    for worker in warmup_workers {
        let result = worker.await?;
        warmup.attempts += result.attempts;
        warmup.successes += result.successes;
        warmup.errors += result.errors;
    }
    if warmup.errors != 0 || warmup.attempts != warmup.successes {
        return Err(Box::new(CliError(
            "bench-write warmup has failed or unverified inserts".into(),
        )));
    }
    let measured_start_sequence = sequence.load(Ordering::Relaxed);
    let measured_prefix = format!("{}-measure-", args.id);
    let before = count_consistent(client, &measured_prefix).await?;
    let started = Instant::now();
    let mut workers = Vec::with_capacity(args.concurrency);
    for _ in 0..args.concurrency {
        workers.push(tokio::spawn(closed_loop(
            shared.clone(),
            args.id.clone(),
            args.value.clone(),
            "measure",
            sequence.clone(),
            args.measure,
        )));
    }
    let mut measured = BenchStats::default();
    for worker in workers {
        let result = worker.await?;
        measured.attempts += result.attempts;
        measured.successes += result.successes;
        measured.errors += result.errors;
        measured.latencies_ns.extend(result.latencies_ns);
    }
    let elapsed = started.elapsed();
    let measured_end_sequence = sequence.load(Ordering::Relaxed);
    let ledger = value_ledger_consistent(client, &measured_prefix).await?;
    let row_count_delta = ledger
        .count
        .checked_sub(before)
        .ok_or_else(|| CliError("bench-write measured row count decreased".into()))?;
    let value_ledger_verified = ledger.min.as_deref() == Some(args.value.as_str())
        && ledger.max.as_deref() == Some(args.value.as_str());
    if measured.errors != 0
        || measured.attempts != measured.successes
        || row_count_delta != measured.successes
        || !value_ledger_verified
    {
        return Err(Box::new(CliError(
            "bench-write attempts/successes/errors or measured row-count delta rejected".into(),
        )));
    }
    let final_sequence = measured_end_sequence
        .checked_sub(1)
        .ok_or_else(|| CliError("bench-write made no measured attempts".into()))?;
    if final_sequence < measured_start_sequence {
        return Err(Box::new(CliError(
            "bench-write made no measured attempts".into(),
        )));
    }
    let final_id = format!("{}-measure-{final_sequence:020}", args.id);
    let final_value = bench_value("measure", final_sequence, &args.value);
    let verified = query_consistent(client, &final_id).await?
        == Some(Sentinel {
            id: final_id.clone(),
            value: final_value.clone(),
        });
    if !verified {
        return Err(Box::new(CliError(
            "bench-write final consistent verification failed".into(),
        )));
    }
    let mut p50 = measured.latencies_ns.clone();
    let mut p95 = measured.latencies_ns.clone();
    let mut p99 = measured.latencies_ns.clone();
    let mut p999 = measured.latencies_ns.clone();
    let max = measured.latencies_ns.iter().max().copied();
    Ok(json!({
        "schema_version": 1, "command": "bench-write", "workload": "d1_sql_unique_request_id_deterministic_write",
        "durability": "Hiqlite Immediate", "payload_bytes": 128, "id_prefix": args.id, "insert_only": true, "unique_request_ids": true,
        "warmup_seconds": args.warmup.as_secs(), "measure_seconds_requested": args.measure.as_secs(), "concurrency": args.concurrency,
        "closed_loop": true, "retries": 0, "successes_after_valid_response": true,
        "warmup": {"attempts":warmup.attempts,"successes":warmup.successes,"errors":warmup.errors},
        "measurement": {"attempts":measured.attempts,"successes":measured.successes,"errors":measured.errors,
          "elapsed_seconds":elapsed.as_secs_f64(), "successes_per_second": measured.successes as f64 / elapsed.as_secs_f64(),
          "latency_ns":{"p50":percentile_ns(&mut p50,50,100),"p95":percentile_ns(&mut p95,95,100),"p99":percentile_ns(&mut p99,99,100),"p999":percentile_ns(&mut p999,999,1000),"max":max}},
        "measured_start_sequence": measured_start_sequence, "measured_end_sequence": measured_end_sequence,
        "row_count_delta": row_count_delta, "final_consistent_verification": verified,
        "value_ledger_verified": value_ledger_verified,
        "final_id": final_id, "final_value": final_value
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    let client = Client::remote(args.nodes, false, false, args.secret, true, None, None).await?;

    match args.command.as_str() {
        "execute" => {
            require_command_args(&args.command_args, 2, "execute")?;
            ensure_schema(&client).await?;
            let id = &args.command_args[0];
            let value = &args.command_args[1];
            let changed = client
                .execute(
                    "INSERT INTO hiqlite_recovery_sentinel (id, value) VALUES ($1, $2) \
                     ON CONFLICT(id) DO UPDATE SET value = excluded.value",
                    params([id.into(), value.into()]),
                )
                .await?;
            print_json(json!({
                "command": "execute",
                "acknowledged": true,
                "changed": changed,
                "id": id,
                "value": value,
            }))?;
        }
        "reset" => {
            require_command_args(&args.command_args, 0, "reset")?;
            ensure_schema(&client).await?;
            let changed = client
                .execute("DELETE FROM hiqlite_recovery_sentinel", Params::new())
                .await?;
            print_json(json!({
                "command": "reset",
                "acknowledged": true,
                "changed": changed,
            }))?;
        }
        "query-local" => {
            require_command_args(&args.command_args, 1, "query-local")?;
            let id = &args.command_args[0];
            print_json(sentinel_json(
                "query-local",
                id,
                query_local(&client, id).await?,
            ))?;
        }
        "query-consistent" => {
            require_command_args(&args.command_args, 1, "query-consistent")?;
            let id = &args.command_args[0];
            print_json(sentinel_json(
                "query-consistent",
                id,
                query_consistent(&client, id).await?,
            ))?;
        }
        "backup" => {
            require_command_args(&args.command_args, 0, "backup")?;
            client.backup().await?;
            print_json(json!({
                "command": "backup",
                "triggered": true,
                "completed": false,
                "note": "S3 upload is asynchronous; verify the external object separately",
            }))?;
        }
        "metrics" => {
            require_command_args(&args.command_args, 0, "metrics")?;
            let metrics = client.metrics_db().await?;
            let mut voter_ids = metrics.membership_config.voter_ids().collect::<Vec<_>>();
            voter_ids.sort_unstable();
            let mut node_ids = metrics
                .membership_config
                .nodes()
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            node_ids.sort_unstable();
            let learner_ids = node_ids
                .iter()
                .copied()
                .filter(|id| !voter_ids.contains(id))
                .collect::<Vec<_>>();
            print_json(json!({
                "command": "metrics",
                "node_id": metrics.id,
                "state": format!("{:?}", metrics.state),
                "running": metrics.running_state.is_ok(),
                "current_term": metrics.current_term,
                "current_leader": metrics.current_leader,
                "last_log_index": metrics.last_log_index,
                "last_applied": metrics.last_applied.map(|id| format!("{id:?}")),
                "voter_ids": voter_ids,
                "learner_ids": learner_ids,
                "node_ids": node_ids,
            }))?;
        }
        "verify-sentinel" => {
            require_command_args(&args.command_args, 2, "verify-sentinel")?;
            let expected = Sentinel {
                id: args.command_args[0].clone(),
                value: args.command_args[1].clone(),
            };
            let local = query_local(&client, &expected.id).await?;
            let consistent = query_consistent(&client, &expected.id).await?;
            if local.as_ref() != Some(&expected) || consistent.as_ref() != Some(&expected) {
                return Err(Box::new(CliError(format!(
                    "sentinel mismatch: expected={expected:?} local={local:?} consistent={consistent:?}"
                ))) as Box<dyn Error>);
            }
            print_json(json!({
                "command": "verify-sentinel",
                "verified": true,
                "id": expected.id,
                "value": expected.value,
                "local": true,
                "consistent": true,
            }))?;
        }
        "bench-write" => print_json(run_bench(&client, bench_args(&args.command_args)?).await?)?,
        command => return Err(Box::new(CliError(format!("unknown command: {command}")))),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_bench_args() -> Vec<String> {
        vec![
            "--warmup-seconds".into(),
            "10".into(),
            "--measure-seconds".into(),
            "60".into(),
            "--concurrency".into(),
            "4".into(),
            "--id".into(),
            "run-01".into(),
            "--value".into(),
            "x".repeat(128),
        ]
    }

    #[test]
    fn bench_args_accepts_the_d1_contract() {
        let parsed = bench_args(&valid_bench_args()).expect("valid D1 arguments");
        assert_eq!(parsed.warmup, Duration::from_secs(10));
        assert_eq!(parsed.measure, Duration::from_secs(60));
        assert_eq!(parsed.concurrency, 4);
        assert_eq!(parsed.id, "run-01");
        assert_eq!(parsed.value.len(), 128);
    }

    #[test]
    fn bench_args_rejects_unsafe_ids_and_wrong_payload_sizes() {
        let mut unsafe_id = valid_bench_args();
        unsafe_id[7] = "run/01".into();
        assert!(bench_args(&unsafe_id).is_err());

        let mut short_value = valid_bench_args();
        short_value[9] = "x".repeat(127);
        assert!(bench_args(&short_value).is_err());
    }

    #[test]
    fn bench_values_are_exact_fixed_and_deterministic() {
        let first = bench_value("measure", 7, &"x".repeat(128));
        let repeated = bench_value("measure", 7, &"x".repeat(128));
        let next = bench_value("measure", 8, &"x".repeat(128));
        assert_eq!(first.len(), 128);
        assert!(first.is_ascii());
        assert_eq!(first, repeated);
        assert_eq!(first, next);
    }

    #[test]
    fn percentile_uses_nearest_rank_without_pooling() {
        let sample = [40, 10, 30, 20];
        assert_eq!(percentile_ns(&mut sample.clone(), 50, 100), Some(20));
        assert_eq!(percentile_ns(&mut sample.clone(), 95, 100), Some(40));
        assert_eq!(percentile_ns(&mut sample.clone(), 999, 1000), Some(40));
        assert_eq!(percentile_ns(&mut [], 50, 100), None);
    }
}
