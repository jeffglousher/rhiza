use std::{
    env,
    process::{self, Command, Stdio},
    sync::{
        atomic::{AtomicU64, AtomicU8, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use reqwest::{blocking::Client, StatusCode};
use rhiza_bench::{
    fault_window_durations, operation_is_write, parse_config, rate_decision, Config, FaultConfig,
    RateDecision, Stats, StatsOutput,
};
use serde::Serialize;
use serde_json::{json, Value};

const BENCH_SEED_ID: &str = "rhiza-bench-seed";

const VERSION_HEADER: &str = "x-rhiza-version";
const PROTOCOL_VERSION: &str = "1";
const BEFORE: u8 = 0;
const DURING: u8 = 1;
const AFTER: u8 = 2;
const UNFAULTED: u8 = 3;
const WORKER_PANIC_DIAGNOSTIC: &str = "benchmark worker panicked; recorded as worker_panic";

fn main() {
    let config = match parse_config(env::args().skip(1), |key| env::var(key).ok()) {
        Ok(config) => config,
        Err(error) if error == "help requested" => {
            print_usage();
            return;
        }
        Err(error) => {
            eprintln!("configuration error: {error}");
            print_usage();
            process::exit(2);
        }
    };

    if let Err(error) = run(config) {
        eprintln!("benchmark failed: {error}");
        process::exit(1);
    }
}

fn run(config: Config) -> Result<(), String> {
    validate_instant_duration("warmup", config.warmup)?;
    validate_instant_duration("measurement", config.duration)?;
    validate_instant_duration("request timeout", config.request_timeout)?;
    validate_instant_duration("fault timeout", config.fault_timeout)?;
    let client = Client::builder()
        .timeout(config.request_timeout)
        .build()
        .map_err(|error| format!("build HTTP client: {error}"))?;
    let run_id = format!(
        "bench-{}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        process::id()
    );
    if !config.skip_setup {
        setup_table(&client, &config, &run_id)?;
    }

    let warmup = run_phase(&client, &config, &run_id, "warmup", config.warmup, None)?;
    let d1_measurement_prefix = format!("{run_id}-measurement-");
    let d1_before = if config.d1_exact_write {
        count_request_rows(&client, &config, &d1_measurement_prefix)?
    } else {
        0
    };
    let measurement = run_phase(
        &client,
        &config,
        &run_id,
        "measurement",
        config.duration,
        config.fault.clone(),
    )?;
    let run_failure = benchmark_failure(&warmup, &measurement);
    let measurement_totals = measurement.stats.output(config.duration);
    let d1 = if config.d1_exact_write {
        let (after, min_value, max_value) = value_ledger(&client, &config, &d1_measurement_prefix)?;
        let row_count_delta = after
            .checked_sub(d1_before)
            .ok_or("D1 row count decreased")?;
        let expected_value = write_value(&config, "");
        let final_id = latest_request_id(&client, &config, &d1_measurement_prefix)?;
        read_request(&client, &config, &final_id)?;
        if measurement_totals.errors != 0
            || measurement_totals.attempts != measurement_totals.successes
            || row_count_delta != measurement_totals.successes
            || min_value.as_deref() != Some(expected_value.as_str())
            || max_value.as_deref() != Some(expected_value.as_str())
        {
            return Err("D1 attempts/successes/errors or row-count delta rejected".into());
        }
        let final_value = write_value(&config, &final_id);
        Some(D1Output {
            unique_request_ids: true,
            insert_only: true,
            row_count_delta,
            final_consistent_verification: true,
            final_id,
            final_value,
            value_ledger_verified: true,
            endpoint_count: config.endpoints.len(),
            endpoint_fallback_attempts: 0,
        })
    } else {
        None
    };
    let measurement_window = measurement
        .measurement_window
        .ok_or_else(|| "measurement phase did not record its wall-clock window".to_string())?;
    let report = BenchmarkReport {
        schema_version: 1,
        run_id,
        endpoints: config.endpoints.clone(),
        workload: workload_name(&config),
        configured: ConfigOutput {
            duration_seconds: config.duration.as_secs_f64(),
            warmup_seconds: config.warmup.as_secs_f64(),
            concurrency: config.concurrency,
            target_rate_per_second: config.target_rate,
            request_timeout_seconds: config.request_timeout.as_secs_f64(),
            fault_timeout_seconds: config.fault_timeout.as_secs_f64(),
            table: config.table,
            setup_skipped: config.skip_setup,
            d1_exact_write: config.d1_exact_write,
        },
        warmup: warmup.stats.output(config.warmup),
        measurement: MeasurementOutput {
            measurement_window,
            configured_duration_seconds: config.duration.as_secs_f64(),
            observed_wall_seconds: measurement.wall_elapsed.as_secs_f64(),
            totals: measurement_totals,
            offered_iterations: measurement.offered_iterations,
            completed_iterations: measurement.stats.output(config.duration).attempts,
            dropped_schedule_iterations: measurement.dropped_schedule_iterations,
            fault_windows: measurement
                .windows
                .output(measurement.wall_elapsed, measurement.fault.as_ref()),
        },
        fault: measurement.fault,
        d1,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report)
            .map_err(|error| format!("encode JSON report: {error}"))?
    );
    match run_failure {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn value_ledger(
    client: &Client,
    config: &Config,
    prefix: &str,
) -> Result<(u64, Option<String>, Option<String>), String> {
    let response = protocol_post_failover(
        client,
        config,
        "d1-ledger",
        "/v1/sql/query",
        json!({"statement":{"sql":format!("SELECT COUNT(*), MIN(value), MAX(value) FROM {} WHERE request_id LIKE ?", config.table),"parameters":[{"type":"text","value":format!("{prefix}%")}]},"consistency":"read_barrier","max_rows":1}),
        "sql_query",
    )?;
    let values = response
        .pointer("/rows/0")
        .and_then(Value::as_array)
        .ok_or("invalid D1 ledger response")?;
    let count = values
        .first()
        .and_then(|v| v.get("value"))
        .and_then(Value::as_u64)
        .ok_or("invalid D1 ledger count")?;
    let text = |index: usize| {
        values
            .get(index)
            .and_then(|v| v.get("value"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    };
    Ok((count, text(1), text(2)))
}

fn count_request_rows(client: &Client, config: &Config, prefix: &str) -> Result<u64, String> {
    let response = protocol_post_failover(
        client,
        config,
        "d1-count",
        "/v1/sql/query",
        json!({"statement":{"sql":format!("SELECT COUNT(*) FROM {} WHERE request_id LIKE ?", config.table),"parameters":[{"type":"text","value":format!("{prefix}%")}]},"consistency":"read_barrier","max_rows":1}),
        "sql_query",
    )?;
    response
        .pointer("/rows/0/0/value")
        .and_then(Value::as_u64)
        .ok_or_else(|| "invalid D1 COUNT response".into())
}

fn latest_request_id(client: &Client, config: &Config, prefix: &str) -> Result<String, String> {
    let response = protocol_post_failover(
        client,
        config,
        "d1-final",
        "/v1/sql/query",
        json!({"statement":{"sql":format!("SELECT request_id FROM {} WHERE request_id LIKE ? ORDER BY request_id DESC LIMIT 1", config.table),"parameters":[{"type":"text","value":format!("{prefix}%")}]},"consistency":"read_barrier","max_rows":1}),
        "sql_query",
    )?;
    response
        .pointer("/rows/0/0/value")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| "invalid D1 final row response".into())
}

fn benchmark_failure(warmup: &PhaseResult, measurement: &PhaseResult) -> Option<String> {
    if warmup.worker_panicked || measurement.worker_panicked {
        Some("one or more benchmark workers panicked".into())
    } else {
        measurement.fault.as_ref().and_then(FaultOutput::failure)
    }
}

fn validate_instant_duration(name: &str, duration: Duration) -> Result<(), String> {
    Instant::now()
        .checked_add(duration)
        .map(|_| ())
        .ok_or_else(|| format!("{name} duration exceeds the platform clock range"))
}

fn epoch_seconds(time: SystemTime, boundary: &str) -> Result<f64, String> {
    let seconds = time
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("capture {boundary} wall clock: {error}"))?
        .as_secs_f64();
    if seconds.is_finite() {
        Ok(seconds)
    } else {
        Err(format!("capture {boundary} wall clock: non-finite epoch"))
    }
}

fn measurement_window(
    started_at_epoch_seconds: f64,
    finished_at_epoch_seconds: f64,
) -> Result<MeasurementWindowOutput, String> {
    if !started_at_epoch_seconds.is_finite()
        || !finished_at_epoch_seconds.is_finite()
        || finished_at_epoch_seconds < started_at_epoch_seconds
    {
        return Err("measurement wall-clock window is invalid".into());
    }
    Ok(MeasurementWindowOutput {
        started_at_epoch_seconds,
        finished_at_epoch_seconds,
    })
}

fn setup_table(client: &Client, config: &Config, run_id: &str) -> Result<(), String> {
    let request_id = format!("{run_id}-setup-ddl");
    let body = json!({
        "request_id": request_id,
        "statements": [{
            "sql": format!(
                "CREATE TABLE IF NOT EXISTS {} (request_id TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)",
                config.table
            ),
            "parameters": []
        }]
    });
    protocol_post_failover(
        client,
        config,
        &request_id,
        "/v1/sql/execute",
        body,
        "sql_execute",
    )
    .map_err(|error| format!("create benchmark table: {error}"))?;
    let seed_id = BENCH_SEED_ID;
    let request_id = format!("{run_id}-setup-seed");
    let body = json!({
        "request_id": request_id,
        "statements": [{
            "sql": format!(
                "INSERT INTO {} (request_id, value) VALUES (?, ?) ON CONFLICT(request_id) DO UPDATE SET value = excluded.value RETURNING request_id, value",
                config.table
            ),
            "parameters": [
                {"type": "text", "value": seed_id},
                {"type": "text", "value": format!("value-{seed_id}")}
            ]
        }]
    });
    let response = protocol_post_failover(
        client,
        config,
        &request_id,
        "/v1/sql/execute",
        body,
        "sql_execute",
    )
    .map_err(|error| format!("insert benchmark seed: {error}"))?;
    let returned_id = response
        .pointer("/results/0/returning/rows/0/0/value")
        .and_then(Value::as_str);
    (returned_id == Some(seed_id))
        .then_some(())
        .ok_or_else(|| "insert benchmark seed: invalid RETURNING response".into())
}

fn run_phase(
    client: &Client,
    config: &Config,
    run_id: &str,
    phase: &str,
    duration: Duration,
    fault_config: Option<FaultConfig>,
) -> Result<PhaseResult, String> {
    run_phase_with_startup_delay(
        client,
        config,
        run_id,
        phase,
        duration,
        fault_config,
        Duration::ZERO,
    )
}

fn run_phase_with_startup_delay(
    client: &Client,
    config: &Config,
    run_id: &str,
    phase: &str,
    duration: Duration,
    fault_config: Option<FaultConfig>,
    worker_startup_delay: Duration,
) -> Result<PhaseResult, String> {
    if duration.is_zero() {
        return Ok(PhaseResult::default());
    }
    validate_instant_duration(phase, duration)?;
    let has_fault = fault_config.is_some();
    let participants = config
        .concurrency
        .checked_add(usize::from(has_fault))
        .ok_or_else(|| "phase participant count overflow".to_string())?;
    let phase_gate = Arc::new(PhaseGate::default());
    let ticket = Arc::new(AtomicU64::new(0));
    let fault_stage = Arc::new(AtomicU8::new(if fault_config.is_some() {
        BEFORE
    } else {
        UNFAULTED
    }));
    let mut workers = Vec::with_capacity(config.concurrency);

    for worker_id in 0..config.concurrency {
        let client = client.clone();
        let config = config.clone();
        let run_id = run_id.to_owned();
        let phase = phase.to_owned();
        let phase_gate = Arc::clone(&phase_gate);
        let ticket = Arc::clone(&ticket);
        let fault_stage = Arc::clone(&fault_stage);
        workers.push(thread::spawn(move || {
            if worker_id == 0 {
                thread::sleep(worker_startup_delay);
            }
            let Some(timing) = phase_gate.wait_for_start() else {
                return WorkerStats::default();
            };
            run_worker(
                worker_id,
                WorkerContext {
                    client: &client,
                    config: &config,
                    run_id: &run_id,
                    phase: &phase,
                    start: timing.start,
                    deadline: timing.deadline,
                    ticket: &ticket,
                    fault_stage: &fault_stage,
                },
            )
        }));
    }

    let fault_handle = fault_config.map(|fault| {
        let identity = (fault.tag.clone(), fault.offset);
        let handle = spawn_phase_fault_hook(
            fault,
            Arc::clone(&phase_gate),
            config.fault_timeout,
            Arc::clone(&fault_stage),
        );
        (identity, handle)
    });
    let timing_result = phase_gate.release(participants, phase, duration);

    let mut result = PhaseResult::default();
    for worker in workers {
        match worker.join() {
            Ok(worker_stats) => result.merge(worker_stats),
            Err(_) => {
                eprintln!("{WORKER_PANIC_DIAGNOSTIC}");
                result.worker_panicked = true;
                result
                    .stats
                    .record(Duration::ZERO, false, false, Some("worker_panic"));
            }
        }
    }
    let finished_at_epoch_seconds = timing_result.as_ref().ok().map(|timing| {
        result.wall_elapsed = timing.start.elapsed();
        epoch_seconds(SystemTime::now(), &format!("{phase} end"))
    });
    result.fault = fault_handle.map(|((tag, offset), handle)| match handle.join() {
        Ok(output) => output,
        Err(_) => FaultOutput::thread_panicked(tag, offset),
    });
    let phase_timing = timing_result?;
    let finished_at_epoch_seconds = finished_at_epoch_seconds
        .ok_or_else(|| "phase end wall clock was not captured".to_string())??;
    result.measurement_window = Some(measurement_window(
        phase_timing.started_at_epoch_seconds,
        finished_at_epoch_seconds,
    )?);
    Ok(result)
}

#[derive(Clone, Copy)]
struct PhaseTiming {
    start: Instant,
    deadline: Instant,
    started_at_epoch_seconds: f64,
}

#[derive(Default)]
struct PhaseGate {
    state: Mutex<PhaseGateState>,
    changed: Condvar,
}

#[derive(Default)]
struct PhaseGateState {
    ready: usize,
    released: bool,
    timing: Option<PhaseTiming>,
}

impl PhaseGate {
    fn wait_for_start(&self) -> Option<PhaseTiming> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.ready = state.ready.saturating_add(1);
        self.changed.notify_all();
        while !state.released {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        state.timing
    }

    fn release(
        &self,
        participants: usize,
        phase: &str,
        duration: Duration,
    ) -> Result<PhaseTiming, String> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        while state.ready < participants {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        let phase_start_wall = SystemTime::now();
        let phase_start = Instant::now();
        let timing = epoch_seconds(phase_start_wall, &format!("{phase} start")).and_then(
            |started_at_epoch_seconds| {
                phase_start
                    .checked_add(duration)
                    .map(|deadline| PhaseTiming {
                        start: phase_start,
                        deadline,
                        started_at_epoch_seconds,
                    })
                    .ok_or_else(|| format!("{phase} duration exceeds the platform clock range"))
            },
        );
        state.timing = timing.as_ref().ok().copied();
        state.released = true;
        self.changed.notify_all();
        timing
    }

    #[cfg(test)]
    fn wait_until_ready(&self, participants: usize) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        while state.ready < participants {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
    }
}

struct WorkerContext<'a> {
    client: &'a Client,
    config: &'a Config,
    run_id: &'a str,
    phase: &'a str,
    start: Instant,
    deadline: Instant,
    ticket: &'a AtomicU64,
    fault_stage: &'a AtomicU8,
}

fn run_worker(worker_id: usize, context: WorkerContext<'_>) -> WorkerStats {
    let deadline = context.deadline;
    let mut sequence = 0_u64;
    let mut last_id = BENCH_SEED_ID.to_owned();
    let mut result = WorkerStats::default();

    loop {
        match wait_for_rate(
            context.config.target_rate,
            context.ticket,
            context.start,
            deadline,
        ) {
            RateDecision::Stop => break,
            RateDecision::Dropped => {
                result.offered_iterations += 1;
                result.dropped_schedule_iterations += 1;
                continue;
            }
            RateDecision::Ready => result.offered_iterations += 1,
        }
        let write = operation_is_write(
            &context.config.workload,
            context.config.write_percent,
            worker_id,
            sequence,
        );
        let stage = context.fault_stage.load(Ordering::Acquire);
        let operation_start = Instant::now();
        let outcome = if write {
            let id = write_request_id(context.run_id, context.phase, worker_id, sequence);
            let response = write_request(context.client, context.config, &id);
            if response.is_ok() {
                last_id = id;
            }
            Outcome::from_result(response, true)
        } else {
            Outcome::from_result(
                read_request(context.client, context.config, &last_id),
                false,
            )
        };
        let latency = operation_start.elapsed();
        result.record(stage, latency, outcome);
        sequence += 1;
    }
    result
}

fn write_request_id(run_id: &str, phase: &str, worker_id: usize, sequence: u64) -> String {
    format!("{run_id}-{phase}-w{worker_id}-{sequence}")
}

fn wait_for_rate(
    target_rate: Option<f64>,
    ticket: &AtomicU64,
    start: Instant,
    deadline: Instant,
) -> RateDecision {
    if Instant::now() >= deadline {
        return RateDecision::Stop;
    }
    let Some(rate) = target_rate else {
        return RateDecision::Ready;
    };
    let sequence = match ticket.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        value.checked_add(1)
    }) {
        Ok(sequence) => sequence,
        Err(_) => return RateDecision::Stop,
    };
    let duration = deadline.saturating_duration_since(start);
    let scheduled_seconds = sequence as f64 / rate;
    if scheduled_seconds >= duration.as_secs_f64() {
        return RateDecision::Stop;
    }
    let Ok(scheduled_offset) = Duration::try_from_secs_f64(scheduled_seconds) else {
        return RateDecision::Stop;
    };
    let Some(scheduled) = start.checked_add(scheduled_offset) else {
        return RateDecision::Stop;
    };
    if scheduled >= deadline {
        return RateDecision::Stop;
    }
    let interval = Duration::try_from_secs_f64(1.0 / rate).unwrap_or(duration);
    if let Some(wait) = scheduled.checked_duration_since(Instant::now()) {
        thread::sleep(wait);
    }
    rate_decision(
        Instant::now().saturating_duration_since(start),
        scheduled.saturating_duration_since(start),
        interval,
        duration,
    )
}

fn write_request(client: &Client, config: &Config, request_id: &str) -> Result<(), String> {
    let value = write_value(config, request_id);
    let body = json!({
        "request_id": request_id,
        "statements": [{
            "sql": format!(
                "INSERT INTO {} (request_id, value) VALUES (?, ?) RETURNING request_id, value",
                config.table
            ),
            "parameters": [
                {"type": "text", "value": request_id},
                {"type": "text", "value": value}
            ]
        }]
    });
    let response = protocol_post_failover(
        client,
        config,
        request_id,
        "/v1/sql/execute",
        body,
        "sql_execute",
    )?;
    let returned_id = response
        .pointer("/results/0/returning/rows/0/0/value")
        .and_then(Value::as_str);
    if returned_id == Some(request_id) {
        Ok(())
    } else {
        Err("invalid_returning_response".into())
    }
}

fn read_request(client: &Client, config: &Config, request_id: &str) -> Result<(), String> {
    let body = json!({
        "statement": {
            "sql": format!("SELECT request_id, value FROM {} WHERE request_id = ?", config.table),
            "parameters": [{"type": "text", "value": request_id}]
        },
        "consistency": "read_barrier",
        "max_rows": 1
    });
    let response = protocol_post_failover(
        client,
        config,
        request_id,
        "/v1/sql/query",
        body,
        "sql_query",
    )?;
    if response.get("columns").is_none() || response.get("rows").is_none() {
        return Err("invalid_sql_query_response".into());
    }
    validate_read_response(&response, config, request_id)
}

fn protocol_post_failover(
    client: &Client,
    config: &Config,
    key: &str,
    path: &str,
    body: Value,
    operation: &str,
) -> Result<Value, String> {
    let candidates = endpoint_candidates(&config.endpoints, key, path);
    let last_index = candidates.len().saturating_sub(1);
    for (index, url) in candidates.into_iter().enumerate() {
        match protocol_post(client, url, &config.token, body.clone(), operation) {
            Ok(response) => return Ok(response),
            Err(error) if index < last_index && retryable_endpoint_error(&error) => continue,
            Err(error) => return Err(error),
        }
    }
    Err("connect".into())
}

fn retryable_endpoint_error(error: &str) -> bool {
    matches!(
        error,
        "connect" | "timeout" | "transport" | "http_429" | "http_502" | "http_503" | "http_504"
    )
}

fn endpoint_candidates(endpoints: &[String], _key: &str, path: &str) -> Vec<String> {
    endpoints.iter().map(|base| endpoint(base, path)).collect()
}

fn endpoint(base: &str, path: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), path)
}

fn write_value(config: &Config, request_id: &str) -> String {
    if config.d1_exact_write {
        "d1-fixed-payload".repeat(128_usize.div_ceil("d1-fixed-payload".len()))[..128].to_owned()
    } else {
        format!("value-{request_id}")
    }
}

fn validate_read_response(
    response: &Value,
    config: &Config,
    request_id: &str,
) -> Result<(), String> {
    let expected = json!([[
        {"type": "text", "value": request_id},
        {"type": "text", "value": write_value(config, request_id)}
    ]]);
    if response.get("rows") == Some(&expected) {
        Ok(())
    } else {
        Err("invalid_sql_query_response".into())
    }
}

fn protocol_post(
    client: &Client,
    url: String,
    token: &str,
    body: Value,
    operation: &str,
) -> Result<Value, String> {
    let response = client
        .post(url)
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth(token)
        .json(&body)
        .send()
        .map_err(classify_transport_error)?;
    if !response.status().is_success() {
        return Err(http_error(response.status()));
    }
    let value: Value = response
        .json()
        .map_err(|_| format!("invalid_{operation}_json"))?;
    if value.get("applied_index").is_none() && operation == "sql_execute" {
        return Err("invalid_sql_execute_response".into());
    }
    Ok(value)
}

fn classify_transport_error(error: reqwest::Error) -> String {
    if error.is_timeout() {
        "timeout".into()
    } else if error.is_connect() {
        "connect".into()
    } else {
        "transport".into()
    }
}

fn http_error(status: StatusCode) -> String {
    format!("http_{}", status.as_u16())
}

#[cfg(test)]
fn spawn_fault_hook(
    fault: FaultConfig,
    start: Instant,
    measurement_deadline: Instant,
    fault_timeout: Duration,
    stage: Arc<AtomicU8>,
) -> thread::JoinHandle<FaultOutput> {
    thread::spawn(move || run_fault_hook(fault, start, measurement_deadline, fault_timeout, stage))
}

fn spawn_phase_fault_hook(
    fault: FaultConfig,
    phase_gate: Arc<PhaseGate>,
    fault_timeout: Duration,
    stage: Arc<AtomicU8>,
) -> thread::JoinHandle<FaultOutput> {
    thread::spawn(move || {
        let Some(timing) = phase_gate.wait_for_start() else {
            return FaultOutput::not_started(fault.tag, fault.offset);
        };
        run_fault_hook(fault, timing.start, timing.deadline, fault_timeout, stage)
    })
}

fn run_fault_hook(
    fault: FaultConfig,
    start: Instant,
    measurement_deadline: Instant,
    fault_timeout: Duration,
    stage: Arc<AtomicU8>,
) -> FaultOutput {
    let Some(scheduled_start) = start.checked_add(fault.offset) else {
        return FaultOutput::not_started(fault.tag, fault.offset);
    };
    if let Some(wait) = scheduled_start.checked_duration_since(Instant::now()) {
        thread::sleep(wait);
    }
    if Instant::now() >= measurement_deadline {
        return FaultOutput::not_started(fault.tag, fault.offset);
    }
    stage.store(DURING, Ordering::Release);
    let command_start = Instant::now();
    let command_start_offset = command_start.saturating_duration_since(start);
    let Some(command_deadline) = command_start.checked_add(fault_timeout) else {
        stage.store(AFTER, Ordering::Release);
        return FaultOutput::failed_to_start(
            fault.tag,
            fault.offset,
            command_start_offset,
            command_start.elapsed(),
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "fault timeout exceeds the platform clock range",
            ),
        );
    };
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(&fault.command)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    command.process_group(0);
    let result = command.spawn();
    let mut child = match result {
        Ok(child) => child,
        Err(error) => {
            stage.store(AFTER, Ordering::Release);
            return FaultOutput::failed_to_start(
                fault.tag,
                fault.offset,
                command_start_offset,
                command_start.elapsed(),
                error,
            );
        }
    };
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let _ = terminate_fault_command(&mut child);
                stage.store(AFTER, Ordering::Release);
                return FaultOutput::finished(
                    fault.tag,
                    fault.offset,
                    command_start_offset,
                    command_start.elapsed(),
                    status,
                );
            }
            Ok(None) if Instant::now() >= command_deadline => {
                let kill_error = terminate_fault_command(&mut child);
                let status = child.wait().ok().and_then(|status| status.code());
                stage.store(AFTER, Ordering::Release);
                return FaultOutput::unfinished(
                    fault.tag,
                    fault.offset,
                    command_start_offset,
                    command_start.elapsed(),
                    fault_timeout,
                    status,
                    kill_error,
                );
            }
            Ok(None) => {
                let wait = command_deadline
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(10));
                thread::sleep(wait);
            }
            Err(error) => {
                let _ = terminate_fault_command(&mut child);
                let _ = child.wait();
                stage.store(AFTER, Ordering::Release);
                return FaultOutput::observation_failed(
                    fault.tag,
                    fault.offset,
                    command_start_offset,
                    command_start.elapsed(),
                    error,
                );
            }
        }
    }
}

fn terminate_fault_command(child: &mut process::Child) -> Option<std::io::Error> {
    #[cfg(unix)]
    {
        let group = format!("-{}", child.id());
        let group_error = match Command::new("/bin/kill")
            .args(["-s", "KILL", "--", &group])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
        {
            Ok(status) if status.success() => return None,
            Ok(status) => std::io::Error::other(format!("kill process group exited with {status}")),
            Err(error) => error,
        };
        match child.kill() {
            Ok(()) => Some(group_error),
            Err(shell_error) => Some(std::io::Error::other(format!(
                "{group_error}; kill shell failed: {shell_error}"
            ))),
        }
    }
    #[cfg(not(unix))]
    {
        child.kill().err()
    }
}

struct Outcome {
    success: bool,
    committed_transaction: bool,
    error: Option<String>,
}

impl Outcome {
    fn from_result(result: Result<(), String>, committed_transaction: bool) -> Self {
        match result {
            Ok(()) => Self {
                success: true,
                committed_transaction,
                error: None,
            },
            Err(error) => Self {
                success: false,
                committed_transaction: false,
                error: Some(error),
            },
        }
    }
}

#[derive(Default)]
struct WorkerStats {
    total: Stats,
    before: Stats,
    during: Stats,
    after: Stats,
    offered_iterations: u64,
    dropped_schedule_iterations: u64,
}

impl WorkerStats {
    fn record(&mut self, stage: u8, latency: Duration, outcome: Outcome) {
        self.total.record(
            latency,
            outcome.success,
            outcome.committed_transaction,
            outcome.error.as_deref(),
        );
        let target = match stage {
            BEFORE => &mut self.before,
            DURING => &mut self.during,
            AFTER => &mut self.after,
            _ => return,
        };
        target.record(
            latency,
            outcome.success,
            outcome.committed_transaction,
            outcome.error.as_deref(),
        );
    }
}

#[derive(Default)]
struct FaultWindows {
    before: Stats,
    during: Stats,
    after: Stats,
}

impl FaultWindows {
    fn output(&self, elapsed: Duration, fault: Option<&FaultOutput>) -> FaultWindowsOutput {
        let durations = fault.map_or((elapsed, Duration::ZERO, Duration::ZERO), |fault| {
            let durations = fault_window_durations(
                elapsed,
                Duration::from_secs_f64(
                    fault
                        .command_start_offset_seconds
                        .unwrap_or(fault.offset_seconds),
                ),
                fault.command_elapsed_seconds.map(Duration::from_secs_f64),
                fault.command_completed,
            );
            (durations.before, durations.during, durations.after)
        });
        FaultWindowsOutput {
            before_elapsed_seconds: durations.0.as_secs_f64(),
            during_elapsed_seconds: durations.1.as_secs_f64(),
            after_elapsed_seconds: durations.2.as_secs_f64(),
            before: self.before.output(durations.0),
            during: self.during.output(durations.1),
            after: self.after.output(durations.2),
        }
    }
}

#[derive(Default)]
struct PhaseResult {
    stats: Stats,
    windows: FaultWindows,
    wall_elapsed: Duration,
    fault: Option<FaultOutput>,
    offered_iterations: u64,
    dropped_schedule_iterations: u64,
    worker_panicked: bool,
    measurement_window: Option<MeasurementWindowOutput>,
}

impl PhaseResult {
    fn merge(&mut self, worker: WorkerStats) {
        self.stats.merge(&worker.total);
        self.windows.before.merge(&worker.before);
        self.windows.during.merge(&worker.during);
        self.windows.after.merge(&worker.after);
        self.offered_iterations += worker.offered_iterations;
        self.dropped_schedule_iterations += worker.dropped_schedule_iterations;
    }
}

#[derive(Serialize)]
struct BenchmarkReport {
    schema_version: u8,
    run_id: String,
    endpoints: Vec<String>,
    workload: &'static str,
    configured: ConfigOutput,
    warmup: StatsOutput,
    measurement: MeasurementOutput,
    #[serde(skip_serializing_if = "Option::is_none")]
    fault: Option<FaultOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    d1: Option<D1Output>,
}

#[derive(Serialize)]
struct D1Output {
    unique_request_ids: bool,
    insert_only: bool,
    row_count_delta: u64,
    final_consistent_verification: bool,
    final_id: String,
    final_value: String,
    value_ledger_verified: bool,
    endpoint_count: usize,
    endpoint_fallback_attempts: u64,
}

#[derive(Serialize)]
struct ConfigOutput {
    duration_seconds: f64,
    warmup_seconds: f64,
    concurrency: usize,
    target_rate_per_second: Option<f64>,
    request_timeout_seconds: f64,
    fault_timeout_seconds: f64,
    table: String,
    setup_skipped: bool,
    d1_exact_write: bool,
}

#[derive(Serialize)]
struct MeasurementOutput {
    measurement_window: MeasurementWindowOutput,
    configured_duration_seconds: f64,
    observed_wall_seconds: f64,
    totals: StatsOutput,
    offered_iterations: u64,
    completed_iterations: u64,
    dropped_schedule_iterations: u64,
    fault_windows: FaultWindowsOutput,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct MeasurementWindowOutput {
    started_at_epoch_seconds: f64,
    finished_at_epoch_seconds: f64,
}

#[derive(Serialize)]
struct FaultWindowsOutput {
    before_elapsed_seconds: f64,
    during_elapsed_seconds: f64,
    after_elapsed_seconds: f64,
    before: StatsOutput,
    during: StatsOutput,
    after: StatsOutput,
}

#[derive(Serialize)]
struct FaultOutput {
    tag: String,
    offset_seconds: f64,
    command_start_offset_seconds: Option<f64>,
    window: String,
    status: String,
    command_status: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    command_error: Option<String>,
    command_elapsed_seconds: Option<f64>,
    command_completed: bool,
}

impl FaultOutput {
    fn finished(
        tag: String,
        offset: Duration,
        command_start_offset: Duration,
        elapsed: Duration,
        status: process::ExitStatus,
    ) -> Self {
        let succeeded = status.success();
        Self {
            tag,
            offset_seconds: offset.as_secs_f64(),
            command_start_offset_seconds: Some(command_start_offset.as_secs_f64()),
            window: "during".into(),
            status: if succeeded { "succeeded" } else { "failed" }.into(),
            command_status: status.code(),
            command_error: (!succeeded && status.code().is_none())
                .then(|| "fault command terminated without an exit status".into()),
            command_elapsed_seconds: Some(elapsed.as_secs_f64()),
            command_completed: true,
        }
    }

    fn not_started(tag: String, offset: Duration) -> Self {
        Self {
            tag,
            offset_seconds: offset.as_secs_f64(),
            command_start_offset_seconds: None,
            window: "before".into(),
            status: "unfinished".into(),
            command_status: None,
            command_error: Some("fault command did not start before measurement ended".into()),
            command_elapsed_seconds: None,
            command_completed: false,
        }
    }

    fn failed_to_start(
        tag: String,
        offset: Duration,
        command_start_offset: Duration,
        elapsed: Duration,
        error: std::io::Error,
    ) -> Self {
        Self {
            tag,
            offset_seconds: offset.as_secs_f64(),
            command_start_offset_seconds: Some(command_start_offset.as_secs_f64()),
            window: "during".into(),
            status: "failed".into(),
            command_status: None,
            command_error: Some(format!("start fault command: {error}")),
            command_elapsed_seconds: Some(elapsed.as_secs_f64()),
            command_completed: false,
        }
    }

    fn unfinished(
        tag: String,
        offset: Duration,
        command_start_offset: Duration,
        elapsed: Duration,
        timeout: Duration,
        command_status: Option<i32>,
        kill_error: Option<std::io::Error>,
    ) -> Self {
        let mut error = format!("fault command exceeded configured timeout of {timeout:?}");
        if let Some(kill_error) = kill_error {
            error.push_str(&format!("; kill failed: {kill_error}"));
        }
        Self {
            tag,
            offset_seconds: offset.as_secs_f64(),
            command_start_offset_seconds: Some(command_start_offset.as_secs_f64()),
            window: "during".into(),
            status: "unfinished".into(),
            command_status,
            command_error: Some(error),
            command_elapsed_seconds: Some(elapsed.as_secs_f64()),
            command_completed: false,
        }
    }

    fn observation_failed(
        tag: String,
        offset: Duration,
        command_start_offset: Duration,
        elapsed: Duration,
        error: std::io::Error,
    ) -> Self {
        Self {
            tag,
            offset_seconds: offset.as_secs_f64(),
            command_start_offset_seconds: Some(command_start_offset.as_secs_f64()),
            window: "during".into(),
            status: "failed".into(),
            command_status: None,
            command_error: Some(format!("observe fault command: {error}")),
            command_elapsed_seconds: Some(elapsed.as_secs_f64()),
            command_completed: false,
        }
    }

    fn thread_panicked(tag: String, offset: Duration) -> Self {
        Self {
            tag,
            offset_seconds: offset.as_secs_f64(),
            command_start_offset_seconds: None,
            window: "during".into(),
            status: "failed".into(),
            command_status: None,
            command_error: Some("fault hook thread panicked".into()),
            command_elapsed_seconds: None,
            command_completed: false,
        }
    }

    fn failure(&self) -> Option<String> {
        if self.status == "succeeded" {
            return None;
        }
        if let Some(error) = &self.command_error {
            return Some(format!("fault hook {} failed: {error}", self.tag));
        }
        if !self.command_completed && self.status == "unfinished" {
            return Some(format!("fault hook {} did not finish", self.tag));
        }
        Some(match self.command_status {
            Some(status) => format!("fault hook {} exited with status {status}", self.tag),
            None => format!("fault hook {} failed without an exit status", self.tag),
        })
    }
}

fn workload_name(config: &Config) -> &'static str {
    match config.workload {
        rhiza_bench::Workload::Read => "read",
        rhiza_bench::Workload::Write => "write",
        rhiza_bench::Workload::Mixed => "mixed",
    }
}

fn print_usage() {
    eprintln!(
        "Usage: cargo run --manifest-path bench/Cargo.toml -- [options]\n\
         Required: --endpoint URL (repeatable, or RHIZA_BENCH_ENDPOINT) and --token TOKEN (or RHIZA_CLIENT_TOKEN)\n\
         Options: --duration 30s --warmup 5s --concurrency 1 --target-rate 100\n\
                  --workload read|write|mixed --write-percent 50 --table rhiza_bench\n\
                  --request-timeout 10s --fault-timeout 5m --skip-setup\n\
                  --fault OFFSET TAG COMMAND"
    );
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{BufRead, BufReader, Read, Write},
        net::{TcpListener, TcpStream},
        process,
        sync::{
            atomic::{AtomicU64, AtomicU8, Ordering},
            Arc,
        },
        thread,
        time::{Duration, Instant},
    };

    use reqwest::blocking::Client;
    use rhiza_bench::{Config, FaultConfig, RateDecision, Workload};

    use super::{
        benchmark_failure, endpoint_candidates, run, run_phase, run_phase_with_startup_delay,
        setup_table, spawn_fault_hook, spawn_phase_fault_hook, wait_for_rate, write_request_id,
        FaultWindows, PhaseGate, PhaseResult, BEFORE, WORKER_PANIC_DIAGNOSTIC,
    };

    fn config(endpoint: String) -> Config {
        Config {
            endpoints: vec![endpoint],
            token: "secret".into(),
            duration: Duration::from_secs(1),
            warmup: Duration::ZERO,
            concurrency: 1,
            target_rate: None,
            workload: Workload::Read,
            write_percent: 0,
            table: "rhiza_bench".into(),
            request_timeout: Duration::from_secs(1),
            fault_timeout: Duration::from_secs(1),
            skip_setup: false,
            d1_exact_write: false,
            fault: None,
        }
    }

    fn read_request_body(stream: &mut TcpStream) -> String {
        let mut reader = BufReader::new(stream);
        let mut content_length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                content_length = value.trim().parse().unwrap();
            }
        }
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body).unwrap();
        String::from_utf8(body).unwrap()
    }

    fn respond(stream: &mut TcpStream, status: &str, body: &str) {
        write!(
            stream,
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
    }

    #[test]
    fn write_request_ids_differ_between_warmup_and_measurement() {
        assert_ne!(
            write_request_id("run", "warmup", 2, 7),
            write_request_id("run", "measurement", 2, 7)
        );
    }

    #[test]
    fn endpoint_candidates_preserve_preferred_failover_order() {
        let endpoints = vec![
            "http://n1".to_owned(),
            "http://n2/".to_owned(),
            "http://n3".to_owned(),
        ];
        let candidates = endpoint_candidates(&endpoints, "request-42", "/v1/sql/execute");

        assert_eq!(
            candidates,
            [
                "http://n1/v1/sql/execute",
                "http://n2/v1/sql/execute",
                "http://n3/v1/sql/execute",
            ]
        );
    }

    #[test]
    fn setup_seed_is_idempotent_for_an_existing_table() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let mut seed_seen = false;
            for stream in listener.incoming().take(4) {
                let mut stream = stream.unwrap();
                let body = read_request_body(&mut stream);
                let is_seed = body.contains("RETURNING request_id, value");
                if is_seed && seed_seen && !body.contains("ON CONFLICT") {
                    respond(&mut stream, "409 Conflict", "{}");
                    continue;
                }
                seed_seen |= is_seed;
                let response = if is_seed {
                    r#"{"applied_index":1,"results":[{"returning":{"rows":[[{"type":"text","value":"rhiza-bench-seed"}]]}}]}"#
                } else {
                    r#"{"applied_index":1,"results":[{}]}"#
                };
                respond(&mut stream, "200 OK", response);
            }
        });
        let client = Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        let config = config(endpoint);

        setup_table(&client, &config, "first").unwrap();
        setup_table(&client, &config, "second").unwrap();
        server.join().unwrap();
    }

    #[test]
    fn setup_retries_each_stable_request_on_the_next_endpoint() {
        let first = TcpListener::bind("127.0.0.1:0").unwrap();
        let second = TcpListener::bind("127.0.0.1:0").unwrap();
        let first_endpoint = format!("http://{}", first.local_addr().unwrap());
        let second_endpoint = format!("http://{}", second.local_addr().unwrap());
        let first_server = thread::spawn(move || {
            first
                .incoming()
                .take(2)
                .map(|stream| {
                    let mut stream = stream.unwrap();
                    let body = read_request_body(&mut stream);
                    respond(&mut stream, "503 Service Unavailable", "{}");
                    body
                })
                .collect::<Vec<_>>()
        });
        let second_server = thread::spawn(move || {
            second
                .incoming()
                .take(2)
                .map(|stream| {
                    let mut stream = stream.unwrap();
                    let body = read_request_body(&mut stream);
                    let response = if body.contains("RETURNING request_id, value") {
                        r#"{"applied_index":1,"results":[{"returning":{"rows":[[{"type":"text","value":"rhiza-bench-seed"}]]}}]}"#
                    } else {
                        r#"{"applied_index":1,"results":[{}]}"#
                    };
                    respond(&mut stream, "200 OK", response);
                    body
                })
                .collect::<Vec<_>>()
        });
        let client = Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        let mut config = config(first_endpoint);
        config.endpoints.push(second_endpoint);

        setup_table(&client, &config, "stable").unwrap();

        let first_bodies = first_server.join().unwrap();
        let second_bodies = second_server.join().unwrap();
        assert_eq!(first_bodies, second_bodies);
        assert_eq!(
            first_bodies
                .iter()
                .map(
                    |body| serde_json::from_str::<serde_json::Value>(body).unwrap()["request_id"]
                        .as_str()
                        .unwrap()
                        .to_owned()
                )
                .collect::<Vec<_>>(),
            ["stable-setup-ddl", "stable-setup-seed"]
        );
    }

    #[test]
    fn measurement_window_uses_phase_boundaries_not_warmup_or_fault_join() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            for (index, stream) in listener.incoming().take(2).enumerate() {
                let mut stream = stream.unwrap();
                let _ = read_request_body(&mut stream);
                if index == 1 {
                    thread::sleep(Duration::from_millis(20));
                }
                respond(
                    &mut stream,
                    "200 OK",
                    r#"{"columns":[],"rows":[[{"type":"text","value":"rhiza-bench-seed"},{"type":"text","value":"value-rhiza-bench-seed"}]]}"#,
                );
            }
        });
        let client = Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap();
        let mut config = config(endpoint);
        config.target_rate = Some(0.001);
        let warmup = run_phase(
            &client,
            &config,
            "run",
            "warmup",
            Duration::from_millis(10),
            None,
        )
        .unwrap();
        let marker =
            std::env::temp_dir().join(format!("rhiza-bench-offset-zero-start-{}", process::id()));
        let _ = fs::remove_file(&marker);
        let call_started = Instant::now();
        let worker_startup_delay = Duration::from_millis(100);
        let measurement = run_phase_with_startup_delay(
            &client,
            &config,
            "run",
            "measurement",
            Duration::from_millis(50),
            Some(FaultConfig {
                offset: Duration::ZERO,
                tag: "slow-join".into(),
                command: format!("printf started > '{}'; sleep 0.2", marker.display()),
            }),
            worker_startup_delay,
        )
        .unwrap();
        server.join().unwrap();

        let warmup_window = warmup.measurement_window.unwrap();
        let measurement_window = measurement.measurement_window.unwrap();
        let reported_span = measurement_window.finished_at_epoch_seconds
            - measurement_window.started_at_epoch_seconds;
        let fault_elapsed = Duration::try_from_secs_f64(
            measurement
                .fault
                .as_ref()
                .unwrap()
                .command_elapsed_seconds
                .unwrap(),
        )
        .unwrap();
        assert!(
            measurement_window.started_at_epoch_seconds >= warmup_window.finished_at_epoch_seconds
        );
        assert!((reported_span - measurement.wall_elapsed.as_secs_f64()).abs() < 0.05);
        assert!(Duration::try_from_secs_f64(reported_span).unwrap() < fault_elapsed);
        assert!(call_started.elapsed() >= worker_startup_delay + fault_elapsed);
        assert!(marker.exists());
        let _ = fs::remove_file(marker);
    }

    #[test]
    fn offset_zero_fault_waits_for_phase_release() {
        let marker =
            std::env::temp_dir().join(format!("rhiza-bench-offset-zero-gate-{}", process::id()));
        let _ = fs::remove_file(&marker);
        let phase_gate = Arc::new(PhaseGate::default());
        let handle = spawn_phase_fault_hook(
            FaultConfig {
                offset: Duration::ZERO,
                tag: "offset-zero".into(),
                command: format!("printf started > '{}'", marker.display()),
            },
            Arc::clone(&phase_gate),
            Duration::from_secs(1),
            Arc::new(AtomicU8::new(BEFORE)),
        );

        phase_gate.wait_until_ready(1);
        assert!(!marker.exists());
        let _timing = phase_gate
            .release(1, "measurement", Duration::from_secs(1))
            .unwrap();
        assert_eq!(handle.join().unwrap().status, "succeeded");
        assert!(marker.exists());
        let _ = fs::remove_file(marker);
    }

    #[test]
    fn open_loop_workers_stop_before_sleeping_past_the_deadline() {
        let start = Instant::now();
        let deadline = start + Duration::from_millis(10);
        let ticket = AtomicU64::new(64);

        assert_eq!(
            wait_for_rate(Some(0.001), &ticket, start, deadline),
            RateDecision::Stop
        );
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn saturated_open_loop_ticket_stays_stopped_without_wrapping() {
        let start = Instant::now();
        let deadline = start + Duration::from_secs(1);
        let ticket = AtomicU64::new(u64::MAX);

        assert_eq!(
            wait_for_rate(Some(1.0), &ticket, start, deadline),
            RateDecision::Stop
        );
        assert_eq!(
            wait_for_rate(Some(1.0), &ticket, start, deadline),
            RateDecision::Stop
        );
        assert_eq!(ticket.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn very_high_open_loop_rate_stops_promptly_at_the_deadline() {
        let start = Instant::now();
        let deadline = start + Duration::from_millis(1);
        let ticket = AtomicU64::new(0);

        while wait_for_rate(Some(1.0e12), &ticket, start, deadline) != RateDecision::Stop {}

        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn worker_panic_diagnostic_is_bounded_to_one_line() {
        assert!(WORKER_PANIC_DIAGNOSTIC.len() <= 128);
        assert!(!WORKER_PANIC_DIAGNOSTIC.contains(['\n', '\r']));
    }

    #[test]
    fn benchmark_rejects_phase_duration_outside_instant_range() {
        let mut config = config("http://unused".into());
        config.skip_setup = true;
        config.duration = Duration::try_from_secs_f64(1.8e19).unwrap();

        assert!(run(config).unwrap_err().contains("duration"));
    }

    #[test]
    fn worker_panic_marks_the_reported_run_as_failed() {
        let warmup = PhaseResult {
            worker_panicked: true,
            ..PhaseResult::default()
        };

        assert!(benchmark_failure(&warmup, &PhaseResult::default())
            .unwrap()
            .contains("worker"));
    }

    #[test]
    fn fault_hook_nonzero_exit_is_joined_and_reported_as_failed() {
        let start = Instant::now();
        let output = spawn_fault_hook(
            FaultConfig {
                offset: Duration::ZERO,
                tag: "nonzero".into(),
                command: "exit 7".into(),
            },
            start,
            start + Duration::from_secs(1),
            Duration::from_secs(1),
            Arc::new(AtomicU8::new(BEFORE)),
        )
        .join()
        .expect("fault hook must be observed");

        assert!(output.command_completed);
        assert_eq!(output.command_status, Some(7));
        assert_eq!(output.status, "failed");
        assert!(output.failure().unwrap().contains("status 7"));
        let json = serde_json::to_value(output).unwrap();
        assert_eq!(json["command_completed"], true);
        assert_eq!(json["command_status"], 7);
        assert_eq!(json["status"], "failed");
    }

    #[test]
    fn unfinished_fault_is_killed_joined_and_reported() {
        let start = Instant::now();
        let output = spawn_fault_hook(
            FaultConfig {
                offset: Duration::ZERO,
                tag: "slow".into(),
                command: "sleep 1".into(),
            },
            start,
            start + Duration::from_secs(1),
            Duration::from_millis(40),
            Arc::new(AtomicU8::new(BEFORE)),
        )
        .join()
        .expect("fault hook must be observed");

        assert!(start.elapsed() < Duration::from_millis(500));
        assert!(!output.command_completed);
        assert_eq!(output.status, "unfinished");
        assert!(output.failure().unwrap().contains("40ms"));
        assert_eq!(
            output.command_error.as_deref(),
            Some("fault command exceeded configured timeout of 40ms")
        );
        let json = serde_json::to_value(output).unwrap();
        assert_eq!(json["command_completed"], false);
        assert_eq!(json["status"], "unfinished");
    }

    #[test]
    fn timed_out_fault_kills_descendants_before_returning() {
        let marker =
            std::env::temp_dir().join(format!("rhiza-bench-fault-descendant-{}", process::id()));
        let _ = fs::remove_file(&marker);
        let start = Instant::now();
        let output = spawn_fault_hook(
            FaultConfig {
                offset: Duration::ZERO,
                tag: "descendant".into(),
                command: format!(
                    "(sleep 0.2; printf survived > '{}') & wait",
                    marker.display()
                ),
            },
            start,
            start + Duration::from_secs(1),
            Duration::from_millis(40),
            Arc::new(AtomicU8::new(BEFORE)),
        )
        .join()
        .unwrap();

        assert_eq!(output.status, "unfinished");
        thread::sleep(Duration::from_millis(300));
        assert!(!marker.exists());
    }

    #[test]
    fn successful_fault_cleans_up_background_descendants() {
        let marker = std::env::temp_dir().join(format!(
            "rhiza-bench-fault-success-descendant-{}",
            process::id()
        ));
        let _ = fs::remove_file(&marker);
        let start = Instant::now();
        let output = spawn_fault_hook(
            FaultConfig {
                offset: Duration::ZERO,
                tag: "background".into(),
                command: format!("(sleep 0.2; printf survived > '{}') &", marker.display()),
            },
            start,
            start + Duration::from_secs(1),
            Duration::from_secs(1),
            Arc::new(AtomicU8::new(BEFORE)),
        )
        .join()
        .unwrap();

        assert_eq!(output.status, "succeeded");
        thread::sleep(Duration::from_millis(300));
        assert!(!marker.exists());
    }

    #[test]
    fn fault_windows_use_actual_delayed_command_start() {
        let start = Instant::now()
            .checked_sub(Duration::from_millis(100))
            .unwrap();
        let output = spawn_fault_hook(
            FaultConfig {
                offset: Duration::from_millis(20),
                tag: "delayed".into(),
                command: "true".into(),
            },
            start,
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
            Arc::new(AtomicU8::new(BEFORE)),
        )
        .join()
        .unwrap();
        let windows = FaultWindows::default().output(Duration::from_millis(200), Some(&output));

        assert_eq!(output.offset_seconds, 0.02);
        assert!(output.command_start_offset_seconds.unwrap() >= 0.09);
        assert!(windows.before_elapsed_seconds >= 0.09);
    }
}
