use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use serde::Serialize;

pub mod cost;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Workload {
    Read,
    Write,
    Mixed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FaultConfig {
    pub offset: Duration,
    pub tag: String,
    pub command: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Config {
    pub endpoints: Vec<String>,
    pub token: String,
    pub duration: Duration,
    pub warmup: Duration,
    pub concurrency: usize,
    pub target_rate: Option<f64>,
    pub workload: Workload,
    pub write_percent: u8,
    pub table: String,
    pub request_timeout: Duration,
    pub fault_timeout: Duration,
    pub skip_setup: bool,
    pub d1_exact_write: bool,
    pub retry_ambiguous_writes: bool,
    pub ambiguous_write_retry_timeout: Duration,
    pub fault: Option<FaultConfig>,
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if self.endpoints.is_empty() || self.endpoints.iter().any(|value| value.is_empty()) {
            return Err("provide at least one --endpoint or RHIZA_BENCH_ENDPOINT".into());
        }
        if self.token.is_empty() {
            return Err("provide --token or RHIZA_CLIENT_TOKEN".into());
        }
        if self.duration.is_zero() {
            return Err("--duration must be greater than zero".into());
        }
        if self.fault_timeout.is_zero() {
            return Err("--fault-timeout must be greater than zero".into());
        }
        for (flag, duration) in [
            ("--duration", self.duration),
            ("--warmup", self.warmup),
            ("--request-timeout", self.request_timeout),
            ("--fault-timeout", self.fault_timeout),
            (
                "--ambiguous-write-retry-timeout",
                self.ambiguous_write_retry_timeout,
            ),
        ] {
            if Instant::now().checked_add(duration).is_none() {
                return Err(format!("{flag} exceeds the platform clock range"));
            }
        }
        if self.concurrency == 0 {
            return Err("--concurrency must be greater than zero".into());
        }
        if self.retry_ambiguous_writes && self.ambiguous_write_retry_timeout.is_zero() {
            return Err("--ambiguous-write-retry-timeout must be greater than zero".into());
        }
        if self.d1_exact_write && self.workload != Workload::Write {
            return Err("--d1-exact-write requires --workload write".into());
        }
        if self.d1_exact_write && !self.retry_ambiguous_writes {
            return Err(
                "--d1-exact-write requires --retry-ambiguous-writes for fail-closed qualification"
                    .into(),
            );
        }
        if self.d1_exact_write && self.endpoints.len() != 1 {
            return Err(
                "--d1-exact-write requires exactly one stable Service endpoint".into(),
            );
        }
        if self.table.is_empty()
            || !self
                .table
                .bytes()
                .enumerate()
                .all(|(index, byte)| match byte {
                    b'a'..=b'z' | b'A'..=b'Z' | b'_' => true,
                    b'0'..=b'9' => index > 0,
                    _ => false,
                })
        {
            return Err("--table must be an ASCII SQL identifier".into());
        }
        if let Some(rate) = self.target_rate {
            if !rate.is_finite() || rate <= 0.0 {
                return Err("--target-rate must be a positive finite number".into());
            }
        }
        if let Some(fault) = &self.fault {
            if fault.offset >= self.duration {
                return Err("--fault offset must be before --duration ends".into());
            }
            if fault.tag.is_empty() || fault.command.is_empty() {
                return Err("--fault tag and command must not be empty".into());
            }
        }
        Ok(())
    }
}

pub fn parse_config(
    args: impl IntoIterator<Item = String>,
    env_lookup: impl Fn(&str) -> Option<String>,
) -> Result<Config, String> {
    let mut endpoints = Vec::new();
    let mut token = None;
    let mut duration = None;
    let mut warmup = None;
    let mut concurrency = None;
    let mut target_rate = None;
    let mut workload = None;
    let mut write_percent = None;
    let mut table = None;
    let mut request_timeout = None;
    let mut fault_timeout = None;
    let mut skip_setup = false;
    let mut d1_exact_write = false;
    let mut retry_ambiguous_writes = false;
    let mut ambiguous_write_retry_timeout = None;
    let mut fault = None;
    let values: Vec<_> = args.into_iter().collect();
    let mut index = 0;

    while index < values.len() {
        let flag = &values[index];
        let next = |index: usize| {
            values
                .get(index + 1)
                .cloned()
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        match flag.as_str() {
            "--endpoint" => {
                endpoints.push(next(index)?);
                index += 2;
            }
            "--token" => {
                token = Some(next(index)?);
                index += 2;
            }
            "--duration" => {
                duration = Some(parse_duration(&next(index)?)?);
                index += 2;
            }
            "--warmup" => {
                warmup = Some(parse_duration(&next(index)?)?);
                index += 2;
            }
            "--concurrency" => {
                concurrency = Some(parse_positive_usize(&next(index)?, "--concurrency")?);
                index += 2;
            }
            "--target-rate" => {
                target_rate = Some(
                    next(index)?
                        .parse::<f64>()
                        .map_err(|_| "--target-rate must be a number".to_string())?,
                );
                index += 2;
            }
            "--workload" => {
                workload = Some(parse_workload(&next(index)?)?);
                index += 2;
            }
            "--write-percent" => {
                write_percent =
                    Some(next(index)?.parse::<u8>().map_err(|_| {
                        "--write-percent must be an integer from 0 to 100".to_string()
                    })?);
                index += 2;
            }
            "--table" => {
                table = Some(next(index)?);
                index += 2;
            }
            "--request-timeout" => {
                request_timeout = Some(parse_duration(&next(index)?)?);
                index += 2;
            }
            "--fault-timeout" => {
                fault_timeout = Some(parse_duration(&next(index)?)?);
                index += 2;
            }
            "--skip-setup" => {
                skip_setup = true;
                index += 1;
            }
            "--d1-exact-write" => {
                d1_exact_write = true;
                index += 1;
            }
            "--retry-ambiguous-writes" => {
                retry_ambiguous_writes = true;
                index += 1;
            }
            "--ambiguous-write-retry-timeout" => {
                ambiguous_write_retry_timeout = Some(parse_duration(&next(index)?)?);
                index += 2;
            }
            "--fault" => {
                let offset = values
                    .get(index + 1)
                    .ok_or_else(|| "--fault requires OFFSET TAG COMMAND".to_string())?;
                let tag = values
                    .get(index + 2)
                    .ok_or_else(|| "--fault requires OFFSET TAG COMMAND".to_string())?;
                let command = values
                    .get(index + 3)
                    .ok_or_else(|| "--fault requires OFFSET TAG COMMAND".to_string())?;
                if fault.is_some() {
                    return Err("only one --fault hook is supported per run".into());
                }
                fault = Some(FaultConfig {
                    offset: parse_duration(offset)?,
                    tag: tag.clone(),
                    command: command.clone(),
                });
                index += 4;
            }
            "--help" | "-h" => return Err("help requested".into()),
            _ => return Err(format!("unknown option: {flag}")),
        }
    }

    if endpoints.is_empty() {
        endpoints.extend(
            env_lookup("RHIZA_BENCH_ENDPOINT")
                .or_else(|| env_lookup("RHIZA_ENDPOINT"))
                .unwrap_or_default()
                .split(',')
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned),
        );
    }
    let config = Config {
        endpoints,
        token: token
            .or_else(|| env_lookup("RHIZA_CLIENT_TOKEN"))
            .or_else(|| env_lookup("RHIZA_BENCH_TOKEN"))
            .unwrap_or_default(),
        duration: duration.unwrap_or(Duration::from_secs(30)),
        warmup: warmup.unwrap_or(Duration::from_secs(5)),
        concurrency: concurrency.unwrap_or(1),
        target_rate,
        workload: workload.unwrap_or(Workload::Mixed),
        write_percent: write_percent.unwrap_or(50),
        table: table.unwrap_or_else(|| "rhiza_bench".into()),
        request_timeout: request_timeout.unwrap_or(Duration::from_secs(10)),
        fault_timeout: fault_timeout.unwrap_or(Duration::from_secs(300)),
        skip_setup,
        d1_exact_write,
        retry_ambiguous_writes,
        ambiguous_write_retry_timeout: ambiguous_write_retry_timeout
            .unwrap_or(Duration::from_secs(60)),
        fault,
    };
    if config.write_percent > 100 {
        return Err("--write-percent must be an integer from 0 to 100".into());
    }
    config.validate()?;
    Ok(config)
}

pub fn parse_duration(value: &str) -> Result<Duration, String> {
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 0.001)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1.0)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60.0)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 3_600.0)
    } else {
        (value, 1.0)
    };
    let seconds = number
        .parse::<f64>()
        .map_err(|_| format!("invalid duration: {value}"))?
        * multiplier;
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(format!("invalid duration: {value}"));
    }
    Duration::try_from_secs_f64(seconds).map_err(|_| format!("invalid duration: {value}"))
}

fn parse_positive_usize(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{flag} must be a positive integer"))
}

fn parse_workload(value: &str) -> Result<Workload, String> {
    match value {
        "read" => Ok(Workload::Read),
        "write" => Ok(Workload::Write),
        "mixed" => Ok(Workload::Mixed),
        _ => Err("--workload must be read, write, or mixed".into()),
    }
}

pub fn operation_is_write(
    workload: &Workload,
    write_percent: u8,
    worker: usize,
    sequence: u64,
) -> bool {
    match workload {
        Workload::Read => false,
        Workload::Write => true,
        Workload::Mixed => (mix(worker as u64 ^ sequence) % 100) < u64::from(write_percent),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RateDecision {
    Ready,
    Dropped,
    Stop,
}

pub fn rate_decision(
    elapsed: Duration,
    scheduled: Duration,
    interval: Duration,
    duration: Duration,
) -> RateDecision {
    if scheduled >= duration || elapsed >= duration {
        RateDecision::Stop
    } else if elapsed > scheduled.saturating_add(interval) {
        RateDecision::Dropped
    } else {
        RateDecision::Ready
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FaultWindowDurations {
    pub before: Duration,
    pub during: Duration,
    pub after: Duration,
}

pub fn fault_window_durations(
    elapsed: Duration,
    offset: Duration,
    command_elapsed: Option<Duration>,
    command_completed: bool,
) -> FaultWindowDurations {
    let before = elapsed.min(offset);
    let remaining = elapsed.saturating_sub(before);
    let during = if command_completed {
        command_elapsed.unwrap_or(Duration::ZERO).min(remaining)
    } else {
        remaining
    };
    FaultWindowDurations {
        before,
        during,
        after: remaining.saturating_sub(during),
    }
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    attempts: u64,
    successes: u64,
    committed_transactions: u64,
    errors: BTreeMap<String, u64>,
    histogram_us: BTreeMap<u64, u64>,
}

impl Stats {
    pub fn record(
        &mut self,
        latency: Duration,
        success: bool,
        committed_transaction: bool,
        error_class: Option<&str>,
    ) {
        self.attempts += 1;
        *self
            .histogram_us
            .entry(histogram_bucket(latency))
            .or_default() += 1;
        if success {
            self.successes += 1;
            if committed_transaction {
                self.committed_transactions += 1;
            }
        } else {
            *self
                .errors
                .entry(error_class.unwrap_or("unknown").to_owned())
                .or_default() += 1;
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.attempts += other.attempts;
        self.successes += other.successes;
        self.committed_transactions += other.committed_transactions;
        for (class, count) in &other.errors {
            *self.errors.entry(class.clone()).or_default() += count;
        }
        for (bucket, count) in &other.histogram_us {
            *self.histogram_us.entry(*bucket).or_default() += count;
        }
    }

    pub fn output(&self, elapsed: Duration) -> StatsOutput {
        StatsOutput {
            attempts: self.attempts,
            successes: self.successes,
            errors: self.attempts.saturating_sub(self.successes),
            successful_committed_transactions: self.committed_transactions,
            successful_committed_transactions_per_second: if elapsed.is_zero() {
                0.0
            } else {
                self.committed_transactions as f64 / elapsed.as_secs_f64()
            },
            latency: LatencyOutput {
                histogram_us: self
                    .histogram_us
                    .iter()
                    .map(|(upper_bound_us, count)| HistogramBucket {
                        upper_bound_us: *upper_bound_us,
                        count: *count,
                    })
                    .collect(),
                p50_ms: self.percentile_ms(0.50),
                p95_ms: self.percentile_ms(0.95),
                p99_ms: self.percentile_ms(0.99),
                p99_9_ms: self.percentile_ms(0.999),
            },
            error_classes: self.errors.clone(),
        }
    }

    fn percentile_ms(&self, percentile: f64) -> Option<f64> {
        if self.attempts == 0 {
            return None;
        }
        let wanted = (self.attempts as f64 * percentile).ceil() as u64;
        let mut seen = 0;
        for (bucket, count) in &self.histogram_us {
            seen += count;
            if seen >= wanted {
                return Some(*bucket as f64 / 1_000.0);
            }
        }
        None
    }
}

fn histogram_bucket(latency: Duration) -> u64 {
    latency.as_micros().min(u128::from(u64::MAX)) as u64
}

#[derive(Clone, Debug, Serialize)]
pub struct StatsOutput {
    pub attempts: u64,
    pub successes: u64,
    pub errors: u64,
    pub successful_committed_transactions: u64,
    pub successful_committed_transactions_per_second: f64,
    pub latency: LatencyOutput,
    pub error_classes: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct LatencyOutput {
    pub histogram_us: Vec<HistogramBucket>,
    pub p50_ms: Option<f64>,
    pub p95_ms: Option<f64>,
    pub p99_ms: Option<f64>,
    pub p99_9_ms: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct HistogramBucket {
    pub upper_bound_us: u64,
    pub count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn config_accepts_environment_endpoint_and_fault_hook() {
        let values = BTreeMap::from([
            ("RHIZA_BENCH_ENDPOINT", "http://node-a,http://node-b"),
            ("RHIZA_CLIENT_TOKEN", "secret"),
        ]);
        let config = parse_config(
            [
                "--duration",
                "2s",
                "--warmup",
                "0ms",
                "--concurrency",
                "4",
                "--workload",
                "mixed",
                "--write-percent",
                "25",
                "--fault-timeout",
                "90s",
                "--fault",
                "500ms",
                "restart",
                "echo restart",
            ]
            .into_iter()
            .map(str::to_owned),
            |key| values.get(key).map(|value| value.to_string()),
        )
        .unwrap();

        assert_eq!(config.endpoints, ["http://node-a", "http://node-b"]);
        assert_eq!(config.warmup, Duration::ZERO);
        assert_eq!(config.fault_timeout, Duration::from_secs(90));
        assert_eq!(config.fault.unwrap().tag, "restart");
    }

    #[test]
    fn config_rejects_invalid_table_name() {
        let result = parse_config(
            [
                "--endpoint",
                "http://node",
                "--token",
                "secret",
                "--duration",
                "1s",
                "--table",
                "items;drop",
                "--fault",
                "1s",
                "late",
                "true",
            ]
            .into_iter()
            .map(str::to_owned),
            |_| None,
        );

        assert!(result.unwrap_err().contains("table"));
    }

    #[test]
    fn config_rejects_fault_that_starts_after_measurement() {
        let result = parse_config(
            [
                "--endpoint",
                "http://node",
                "--token",
                "secret",
                "--duration",
                "1s",
                "--fault",
                "1s",
                "late",
                "true",
            ]
            .into_iter()
            .map(str::to_owned),
            |_| None,
        );

        assert!(result.unwrap_err().contains("before --duration"));
    }

    #[test]
    fn durations_accept_hours_and_reject_unrepresentable_finite_values() {
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7_200));
        assert!(parse_duration("1e300s").is_err());
    }

    #[test]
    fn config_rejects_duration_outside_platform_clock_range() {
        let result = parse_config(
            [
                "--endpoint",
                "http://node",
                "--token",
                "secret",
                "--duration",
                "1.8e19s",
            ]
            .into_iter()
            .map(str::to_owned),
            |_| None,
        );

        assert!(result.unwrap_err().contains("platform clock range"));
    }

    #[test]
    fn config_rejects_request_timeout_outside_platform_clock_range() {
        let result = parse_config(
            [
                "--endpoint",
                "http://node",
                "--token",
                "secret",
                "--request-timeout",
                "1.8e19s",
            ]
            .into_iter()
            .map(str::to_owned),
            |_| None,
        );

        assert!(result.unwrap_err().contains("--request-timeout"));
    }

    #[test]
    fn histogram_percentiles_and_transaction_rate_are_aggregated() {
        let mut stats = Stats::default();
        stats.record(Duration::from_micros(90), true, true, None);
        stats.record(Duration::from_micros(150), true, false, None);
        stats.record(Duration::from_micros(250), false, false, Some("http_503"));
        let output = stats.output(Duration::from_secs(2));

        assert_eq!(output.attempts, 3);
        assert_eq!(output.successes, 2);
        assert_eq!(output.errors, 1);
        assert_eq!(output.successful_committed_transactions, 1);
        assert_eq!(output.successful_committed_transactions_per_second, 0.5);
        assert_eq!(output.latency.p50_ms, Some(0.15));
        assert_eq!(output.latency.p95_ms, Some(0.25));
        assert_eq!(output.latency.p99_9_ms, Some(0.25));
        assert_eq!(output.error_classes.get("http_503"), Some(&1));
    }

    #[test]
    fn latency_percentiles_preserve_microsecond_resolution_and_emit_p99_9() {
        let mut stats = Stats::default();
        for _ in 0..998 {
            stats.record(Duration::from_micros(1_001), true, true, None);
        }
        for _ in 0..2 {
            stats.record(Duration::from_micros(12_345), true, true, None);
        }

        let output = stats.output(Duration::from_secs(1));

        assert_eq!(output.latency.p99_ms, Some(1.001));
        assert_eq!(output.latency.p99_9_ms, Some(12.345));
        assert_eq!(
            output
                .latency
                .histogram_us
                .iter()
                .map(|bucket| (bucket.upper_bound_us, bucket.count))
                .collect::<Vec<_>>(),
            vec![(1_001, 998), (12_345, 2)]
        );
    }

    #[test]
    fn mixed_workload_is_deterministic_and_honors_extremes() {
        assert!(operation_is_write(&Workload::Mixed, 100, 0, 1));
        assert!(!operation_is_write(&Workload::Mixed, 0, 0, 1));
        assert!(!operation_is_write(&Workload::Read, 100, 0, 1));
        assert!(operation_is_write(&Workload::Write, 0, 0, 1));
        assert_eq!(
            operation_is_write(&Workload::Mixed, 50, 2, 42),
            operation_is_write(&Workload::Mixed, 50, 2, 42)
        );
    }

    #[test]
    fn fault_window_durations_use_actual_windows_and_no_after_while_running() {
        let completed = fault_window_durations(
            Duration::from_secs(10),
            Duration::from_secs(2),
            Some(Duration::from_secs(3)),
            true,
        );
        assert_eq!(completed.before, Duration::from_secs(2));
        assert_eq!(completed.during, Duration::from_secs(3));
        assert_eq!(completed.after, Duration::from_secs(5));

        let running =
            fault_window_durations(Duration::from_secs(10), Duration::from_secs(2), None, false);
        assert_eq!(running.before, Duration::from_secs(2));
        assert_eq!(running.during, Duration::from_secs(8));
        assert_eq!(running.after, Duration::ZERO);
    }

    #[test]
    fn rate_decision_drops_slots_more_than_one_interval_late() {
        assert_eq!(
            rate_decision(
                Duration::from_secs(3),
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(10),
            ),
            RateDecision::Dropped
        );
        assert_eq!(
            rate_decision(
                Duration::from_millis(1500),
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(10),
            ),
            RateDecision::Ready
        );
        assert_eq!(
            rate_decision(
                Duration::from_secs(10),
                Duration::from_secs(1),
                Duration::from_secs(1),
                Duration::from_secs(10),
            ),
            RateDecision::Stop
        );
    }
}
