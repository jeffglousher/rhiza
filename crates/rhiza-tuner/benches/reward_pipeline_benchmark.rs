//! Reward pipeline comparison benchmark: old (linear) vs new (log + SLO-aware).
//!
//! Scenarios:
//! 1. Normal operation (low latency, no errors)
//! 2. Degraded cluster (mixed latencies)
//! 3. SLO violation burst
//! 4. Flaky node (intermittent timeouts)
//! 5. Recovery from failure

use rhiza_tuner::types::*;

const N_ROUNDS: usize = 500;

// ---------------------------------------------------------------------------
// Scenario definitions
// ---------------------------------------------------------------------------

struct ScenarioEvent {
    latency_us: u64,
    success: bool,
    timeout: bool,
    additional_rpcs: u32,
    duplicate_work: bool,
    contention: bool,
}

fn scenario_normal() -> Vec<ScenarioEvent> {
    // Normal: low latency, no errors, occasional hedging
    (0..N_ROUNDS)
        .map(|i| {
            let latency = 1000 + (i % 500) as u64; // 1-1.5ms
            ScenarioEvent {
                latency_us: latency,
                success: true,
                timeout: false,
                additional_rpcs: if i % 10 == 0 { 1 } else { 0 },
                duplicate_work: i % 10 == 0,
                contention: false,
            }
        })
        .collect()
}

fn scenario_degraded() -> Vec<ScenarioEvent> {
    // Degraded: mixed latencies, some high
    (0..N_ROUNDS)
        .map(|i| {
            let latency = match i % 5 {
                0 => 50_000,  // 50ms (at SLO)
                1 => 75_000,  // 75ms (above SLO)
                2 => 10_000,  // 10ms
                3 => 25_000,  // 25ms
                _ => 100_000, // 100ms (well above SLO)
            };
            ScenarioEvent {
                latency_us: latency,
                success: true,
                timeout: false,
                additional_rpcs: 1,
                duplicate_work: true,
                contention: i % 3 == 0,
            }
        })
        .collect()
}

fn scenario_slo_violation() -> Vec<ScenarioEvent> {
    // SLO violation burst: first half normal, second half SLO breach
    (0..N_ROUNDS)
        .map(|i| {
            let latency = if i < N_ROUNDS / 2 {
                5_000 // 5ms - well under SLO
            } else {
                80_000 // 80ms - SLO violation
            };
            ScenarioEvent {
                latency_us: latency,
                success: true,
                timeout: false,
                additional_rpcs: if i >= N_ROUNDS / 2 { 2 } else { 0 },
                duplicate_work: i >= N_ROUNDS / 2,
                contention: i >= N_ROUNDS / 2 && i % 2 == 0,
            }
        })
        .collect()
}

fn scenario_flaky() -> Vec<ScenarioEvent> {
    // Flaky: alternating success/timeout
    (0..N_ROUNDS)
        .map(|i| {
            if i % 3 == 0 {
                ScenarioEvent {
                    latency_us: 100_000, // timeout
                    success: false,
                    timeout: true,
                    additional_rpcs: 2,
                    duplicate_work: true,
                    contention: false,
                }
            } else {
                ScenarioEvent {
                    latency_us: 2_000, // fast
                    success: true,
                    timeout: false,
                    additional_rpcs: 0,
                    duplicate_work: false,
                    contention: false,
                }
            }
        })
        .collect()
}

fn scenario_recovery() -> Vec<ScenarioEvent> {
    // Recovery: first third degraded, middle third timeout, last third normal
    (0..N_ROUNDS)
        .map(|i| {
            if i < N_ROUNDS / 3 {
                // Degraded phase
                ScenarioEvent {
                    latency_us: 80_000,
                    success: true,
                    timeout: false,
                    additional_rpcs: 1,
                    duplicate_work: true,
                    contention: true,
                }
            } else if i < 2 * N_ROUNDS / 3 {
                // Down phase
                ScenarioEvent {
                    latency_us: 100_000,
                    success: false,
                    timeout: true,
                    additional_rpcs: 2,
                    duplicate_work: true,
                    contention: false,
                }
            } else {
                // Recovery phase
                ScenarioEvent {
                    latency_us: 3_000,
                    success: true,
                    timeout: false,
                    additional_rpcs: 0,
                    duplicate_work: false,
                    contention: false,
                }
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Simulation runner
// ---------------------------------------------------------------------------

struct SimulationResult {
    scenario: &'static str,
    rounds: usize,
    mean_reward: f64,
    min_reward: f64,
    max_reward: f64,
    reward_stddev: f64,
    slo_violations: usize,
    ema_final: f64,
    reward_stability: f64, // coefficient of variation
}

impl std::fmt::Display for SimulationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "  Scenario:         {}", self.scenario)?;
        writeln!(f, "  Rounds:           {}", self.rounds)?;
        writeln!(f, "  Mean reward:      {:.4}", self.mean_reward)?;
        writeln!(f, "  Min reward:       {:.4}", self.min_reward)?;
        writeln!(f, "  Max reward:       {:.4}", self.max_reward)?;
        writeln!(f, "  Stddev:           {:.4}", self.reward_stddev)?;
        writeln!(f, "  Stability (CV):   {:.4}", self.reward_stability)?;
        writeln!(f, "  SLO violations:   {}", self.slo_violations)?;
        writeln!(f, "  EMA final:        {:.4}", self.ema_final)?;
        Ok(())
    }
}

fn print_comparison(scenario: &str, old: &SimulationResult, new: &SimulationResult) {
    println!("\n{}", "=".repeat(70));
    println!("  {scenario}");
    println!("{}", "=".repeat(70));

    println!(
        "\n  {:>20} {:>20} {:>20}",
        "", "Linear (old)", "Log+SLO (new)"
    );
    println!("  {:->20} {:->20} {:->20}", "", "", "");

    let row = |label: &str, ov: String, nv: String| {
        println!("  {:>20} {:>20} {:>20}", label, ov, nv);
    };

    row(
        "Mean reward",
        format!("{:.4}", old.mean_reward),
        format!("{:.4}", new.mean_reward),
    );
    row(
        "Min reward",
        format!("{:.4}", old.min_reward),
        format!("{:.4}", new.min_reward),
    );
    row(
        "Max reward",
        format!("{:.4}", old.max_reward),
        format!("{:.4}", new.max_reward),
    );
    row(
        "Stddev",
        format!("{:.4}", old.reward_stddev),
        format!("{:.4}", new.reward_stddev),
    );
    row(
        "Stability (CV)",
        format!("{:.4}", old.reward_stability),
        format!("{:.4}", new.reward_stability),
    );
    row(
        "SLO violations",
        format!("{}", old.slo_violations),
        format!("{}", new.slo_violations),
    );

    // Improvement calculations
    let mean_imp = if old.mean_reward.abs() > 0.001 {
        (new.mean_reward - old.mean_reward) / old.mean_reward.abs() * 100.0
    } else {
        0.0
    };
    let stability_imp = if old.reward_stability > 0.01 {
        (old.reward_stability - new.reward_stability) / old.reward_stability * 100.0
    } else {
        0.0
    };

    println!("\n  {:>20} {:>20}", "Improvement", "");
    println!("  {:->20} {:->20}", "", "");
    row("Mean reward", format!("{mean_imp:+.1}%"), "".into());
    row("Stability", format!("{stability_imp:+.1}%"), "".into());
}

fn main() {
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║  Reward Pipeline Comparison: Linear vs Log+SLO-Aware           ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Configuration:");
    println!("    SLO latency: 50ms");
    println!("    Rounds: {N_ROUNDS} per scenario");
    println!();

    let slo_latency_us = 50_000; // 50ms SLO

    // Scenario 1: Normal operation
    {
        let events = scenario_normal();
        let old_config = rhiza_tuner::RewardConfig {
            slo_latency_us: u64::MAX, // Disable SLO for "old" behavior
            slo_exponential_factor: 1.0,
            lambda_slo: 0.0,
            latency_base_us: 1_000_000.0, // Large base = near-linear
            ..Default::default()
        };
        let new_config = rhiza_tuner::RewardConfig {
            slo_latency_us,
            ..Default::default()
        };

        let old = run_simulation_with_config("Normal", &events, &old_config);
        let new = run_simulation_with_config("Normal", &events, &new_config);
        print_comparison("1. Normal operation (1-1.5ms latency)", &old, &new);
    }

    // Scenario 2: Degraded cluster
    {
        let events = scenario_degraded();
        let old_config = rhiza_tuner::RewardConfig {
            slo_latency_us: u64::MAX,
            slo_exponential_factor: 1.0,
            lambda_slo: 0.0,
            latency_base_us: 1_000_000.0,
            ..Default::default()
        };
        let new_config = rhiza_tuner::RewardConfig {
            slo_latency_us,
            ..Default::default()
        };

        let old = run_simulation_with_config("Degraded", &events, &old_config);
        let new = run_simulation_with_config("Degraded", &events, &new_config);
        print_comparison("2. Degraded cluster (mixed latencies)", &old, &new);
    }

    // Scenario 3: SLO violation burst
    {
        let events = scenario_slo_violation();
        let old_config = rhiza_tuner::RewardConfig {
            slo_latency_us: u64::MAX,
            slo_exponential_factor: 1.0,
            lambda_slo: 0.0,
            latency_base_us: 1_000_000.0,
            ..Default::default()
        };
        let new_config = rhiza_tuner::RewardConfig {
            slo_latency_us,
            ..Default::default()
        };

        let old = run_simulation_with_config("SLO burst", &events, &old_config);
        let new = run_simulation_with_config("SLO burst", &events, &new_config);
        print_comparison("3. SLO violation burst (5ms → 80ms)", &old, &new);
    }

    // Scenario 4: Flaky node
    {
        let events = scenario_flaky();
        let old_config = rhiza_tuner::RewardConfig {
            slo_latency_us: u64::MAX,
            slo_exponential_factor: 1.0,
            lambda_slo: 0.0,
            latency_base_us: 1_000_000.0,
            ..Default::default()
        };
        let new_config = rhiza_tuner::RewardConfig {
            slo_latency_us,
            ..Default::default()
        };

        let old = run_simulation_with_config("Flaky", &events, &old_config);
        let new = run_simulation_with_config("Flaky", &events, &new_config);
        print_comparison("4. Flaky node (33% timeout)", &old, &new);
    }

    // Scenario 5: Recovery
    {
        let events = scenario_recovery();
        let old_config = rhiza_tuner::RewardConfig {
            slo_latency_us: u64::MAX,
            slo_exponential_factor: 1.0,
            lambda_slo: 0.0,
            latency_base_us: 1_000_000.0,
            ..Default::default()
        };
        let new_config = rhiza_tuner::RewardConfig {
            slo_latency_us,
            ..Default::default()
        };

        let old = run_simulation_with_config("Recovery", &events, &old_config);
        let new = run_simulation_with_config("Recovery", &events, &new_config);
        print_comparison("5. Recovery (degraded → down → normal)", &old, &new);
    }

    // Latency discrimination comparison
    println!("\n{}", "=".repeat(70));
    println!("  6. Latency discrimination comparison");
    println!("{}", "=".repeat(70));

    let latencies = [
        500, 1_000, 2_000, 5_000, 10_000, 25_000, 50_000, 75_000, 100_000,
    ];
    println!(
        "\n  {:>15} {:>15} {:>15} {:>15}",
        "Latency", "Linear", "Log+SLO", "Ratio"
    );
    println!("  {:->15} {:->15} {:->15} {:->15}", "", "", "", "");

    for &lat in &latencies {
        let old_config = rhiza_tuner::RewardConfig {
            slo_latency_us: u64::MAX,
            latency_base_us: 1_000_000.0,
            ..Default::default()
        };
        let new_config = rhiza_tuner::RewardConfig {
            slo_latency_us: 50_000,
            ..Default::default()
        };

        let mut old_pipeline = rhiza_tuner::RewardPipeline::new(old_config);
        let mut new_pipeline = rhiza_tuner::RewardPipeline::new(new_config);

        let outcome = TerminalOutcome::Success {
            decision_latency_us: lat,
            additional_rpcs: 0,
            duplicate_proposer_work: false,
            contention_or_round_escalation: false,
        };

        let old_reward = old_pipeline.compute(&outcome);
        let new_reward = new_pipeline.compute(&outcome);

        let ratio = if old_reward.total.abs() > 0.0001 {
            new_reward.total / old_reward.total
        } else {
            0.0
        };

        println!(
            "  {:>12}µs {:>15.4} {:>15.4} {:>15.2}",
            lat, old_reward.total, new_reward.total, ratio
        );
    }

    println!("\n{}", "=".repeat(70));
    println!("  SIMULATION COMPLETE");
    println!("{}", "=".repeat(70));
}

fn run_simulation_with_config(
    scenario_name: &'static str,
    events: &[ScenarioEvent],
    config: &rhiza_tuner::RewardConfig,
) -> SimulationResult {
    let mut pipeline = rhiza_tuner::RewardPipeline::new(config.clone());

    let mut rewards = Vec::with_capacity(events.len());
    let mut slo_violations = 0usize;

    for event in events {
        let outcome = if event.timeout {
            TerminalOutcome::Timeout
        } else if !event.success {
            TerminalOutcome::Error {
                additional_rpcs: event.additional_rpcs,
                duplicate_proposer_work: event.duplicate_work,
            }
        } else {
            TerminalOutcome::Success {
                decision_latency_us: event.latency_us,
                additional_rpcs: event.additional_rpcs,
                duplicate_proposer_work: event.duplicate_work,
                contention_or_round_escalation: event.contention,
            }
        };

        if event.latency_us > config.slo_latency_us && event.success {
            slo_violations += 1;
        }

        let reward = pipeline.compute(&outcome);
        rewards.push(reward.total);
    }

    let mean = rewards.iter().sum::<f64>() / rewards.len() as f64;
    let min = rewards.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = rewards.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let variance = rewards.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / rewards.len() as f64;
    let stddev = variance.sqrt();
    let cv = if mean.abs() > 0.001 {
        stddev / mean.abs()
    } else {
        0.0
    };

    SimulationResult {
        scenario: scenario_name,
        rounds: events.len(),
        mean_reward: mean,
        min_reward: min,
        max_reward: max,
        reward_stddev: stddev,
        slo_violations,
        ema_final: pipeline.ema_reward(),
        reward_stability: cv,
    }
}
