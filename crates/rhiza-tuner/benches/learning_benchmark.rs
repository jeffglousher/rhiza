//! MAB learning comparison: old (linear) vs new (log+SLO) reward pipeline.
//!
//! Tests:
//! 1. Convergence speed - how fast the tuner learns the optimal proposer
//! 2. Accuracy - how often the tuner picks the optimal proposer
//! 3. Adaptation - how fast the tuner adapts to topology changes
//! 4. Stability - variance in proposer selection over time

use std::time::Duration;

use rhiza_tuner::types::*;
use rhiza_tuner::{MabTuner, RolloutStage, TunerConfig};

const N_ROUNDS: usize = 2000;
const N_PEERS: usize = 5;
const WARMUP_SAMPLES: usize = 100;

// ---------------------------------------------------------------------------
// Node simulation
// ---------------------------------------------------------------------------

struct SimulatedNode {
    name: NodeId,
    base_latency_us: u64,
    failure_rate: f64,
}

impl SimulatedNode {
    fn new(name: &str, base_latency_us: u64, failure_rate: f64) -> Self {
        Self {
            name: name.into(),
            base_latency_us,
            failure_rate,
        }
    }

    fn sample(&self, round: usize) -> (u64, bool) {
        let jitter = ((round * 7 + 13) % 200) as i64 - 100;
        let latency = (self.base_latency_us as i64 + jitter).max(100) as u64;
        let timeout = (round * 31 % 1000) as f64 / 1000.0 < self.failure_rate;
        if timeout {
            (100_000, true)
        } else {
            (latency, false)
        }
    }
}

fn peer_names() -> Vec<NodeId> {
    (0..N_PEERS).map(|i| format!("peer-{i}")).collect()
}

fn test_identity() -> Identity {
    Identity {
        cluster_id: "learn-test".into(),
        epoch: 1,
        config_id: 1,
        membership_digest: [0u8; 32],
        recovery_generation: 0,
    }
}

fn candidate_set() -> CandidateSet {
    CandidateSet {
        identity: test_identity(),
        eligible_voters: peer_names(),
        hedge_delay_allowlist: vec![
            HedgeDelayBucket::Ms5,
            HedgeDelayBucket::Ms10,
            HedgeDelayBucket::Ms25,
            HedgeDelayBucket::Ms50,
            HedgeDelayBucket::Ms100,
            HedgeDelayBucket::Static,
        ],
        static_hedge_delay_ms: 100,
    }
}

// ---------------------------------------------------------------------------
// Learning simulation
// ---------------------------------------------------------------------------

struct LearningResult {
    pipeline_name: &'static str,
    scenario: &'static str,
    total_rounds: usize,
    optimal_picks: usize,
    optimal_rate: f64,
    convergence_round: usize, // round where optimal rate > 80%
    mean_reward: f64,
    reward_stddev: f64,
    adaptation_speed: usize, // rounds to recover after topology change
}

impl std::fmt::Display for LearningResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "  Pipeline:         {}", self.pipeline_name)?;
        writeln!(f, "  Scenario:         {}", self.scenario)?;
        writeln!(f, "  Rounds:           {}", self.total_rounds)?;
        writeln!(f, "  Optimal picks:    {} ({:.1}%)", self.optimal_picks, self.optimal_rate * 100.0)?;
        writeln!(f, "  Convergence:      round {}", self.convergence_round)?;
        writeln!(f, "  Mean reward:      {:.4}", self.mean_reward)?;
        writeln!(f, "  Reward stddev:    {:.4}", self.reward_stddev)?;
        writeln!(f, "  Adaptation speed: {} rounds", self.adaptation_speed)?;
        Ok(())
    }
}

fn run_learning_scenario(
    pipeline_name: &'static str,
    scenario: &'static str,
    nodes: &[SimulatedNode],
    optimal_idx: usize,
    reward_config: rhiza_tuner::RewardConfig,
) -> LearningResult {
    let tuner_config = TunerConfig {
        cold_start_min_samples: WARMUP_SAMPLES as u64,
        ..Default::default()
    };
    let tuner = MabTuner::with_stage(tuner_config, RolloutStage::DefaultOn);
    let identity = test_identity();
    let names = peer_names();
    let candidates = candidate_set();

    // Warmup: feed initial telemetry
    for round in 0..WARMUP_SAMPLES {
        for (idx, node) in nodes.iter().enumerate() {
            let (lat, timeout) = node.sample(round);
            tuner
                .collector()
                .record_proposer_latency(&names[idx], lat, !timeout, timeout);
        }
    }

    let mut optimal_picks = 0usize;
    let mut rewards = Vec::with_capacity(N_ROUNDS);
    let mut convergence_round = N_ROUNDS;
    let mut consecutive_optimal = 0usize;
    let mut adaptation_start = 0usize;
    let mut adaptation_speed = 0usize;

    for round in 0..N_ROUNDS {
        let cid = format!("{scenario}-{round}");
        let result = tuner.select_action(&identity, &names, &candidates, &cid);

        let selected = names
            .iter()
            .position(|n| *n == result.output.action.first_request_target)
            .unwrap_or(0);

        // Simulate outcome
        let (lat, timeout) = nodes[selected].sample(round);
        let outcome = if timeout {
            TerminalOutcome::Timeout
        } else {
            TerminalOutcome::Success {
                decision_latency_us: lat,
                additional_rpcs: 0,
                duplicate_proposer_work: false,
                contention_or_round_escalation: false,
            }
        };

        // Record outcome
        tuner.record_outcome(&cid, &result.output.action, &outcome);
        tuner
            .collector()
            .record_proposer_latency(&names[selected], lat, !timeout, timeout);

        // Track optimal picks
        if selected == optimal_idx {
            optimal_picks += 1;
            consecutive_optimal += 1;
            if consecutive_optimal >= 50 && convergence_round == N_ROUNDS {
                convergence_round = round;
            }
        } else {
            consecutive_optimal = 0;
            // Track adaptation after topology change
            if round > 0 && adaptation_start == 0 {
                adaptation_start = round;
            }
        }

        // Track adaptation speed
        if adaptation_start > 0 && selected == optimal_idx && adaptation_speed == 0 {
            adaptation_speed = round - adaptation_start;
        }

        // Compute reward for tracking
        let mut pipeline = rhiza_tuner::RewardPipeline::new(reward_config.clone());
        let reward = pipeline.compute(&outcome);
        rewards.push(reward.total);
    }

    let mean = rewards.iter().sum::<f64>() / rewards.len() as f64;
    let variance = rewards.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / rewards.len() as f64;

    LearningResult {
        pipeline_name,
        scenario,
        total_rounds: N_ROUNDS,
        optimal_picks,
        optimal_rate: optimal_picks as f64 / N_ROUNDS as f64,
        convergence_round,
        mean_reward: mean,
        reward_stddev: variance.sqrt(),
        adaptation_speed,
    }
}

fn print_comparison(scenario: &str, old: &LearningResult, new: &LearningResult) {
    println!("\n{}", "=".repeat(70));
    println!("  {scenario}");
    println!("{}", "=".repeat(70));

    println!("\n  {:>20} {:>20} {:>20}", "", "Linear (old)", "Log+SLO (new)");
    println!("  {:->20} {:->20} {:->20}", "", "", "");

    let row = |label: &str, ov: String, nv: String| {
        println!("  {:>20} {:>20} {:>20}", label, ov, nv);
    };

    row("Optimal picks",
        format!("{} ({:.1}%)", old.optimal_picks, old.optimal_rate * 100.0),
        format!("{} ({:.1}%)", new.optimal_picks, new.optimal_rate * 100.0));
    row("Convergence",
        format!("round {}", old.convergence_round),
        format!("round {}", new.convergence_round));
    row("Mean reward",
        format!("{:.4}", old.mean_reward),
        format!("{:.4}", new.mean_reward));
    row("Reward stddev",
        format!("{:.4}", old.reward_stddev),
        format!("{:.4}", new.reward_stddev));
    row("Adaptation speed",
        format!("{} rounds", old.adaptation_speed),
        format!("{} rounds", new.adaptation_speed));

    // Improvement calculations
    let optimal_imp = if old.optimal_rate > 0.0 {
        (new.optimal_rate - old.optimal_rate) / old.optimal_rate * 100.0
    } else {
        0.0
    };
    let convergence_imp = if old.convergence_round > 0 {
        (old.convergence_round as f64 - new.convergence_round as f64) / old.convergence_round as f64 * 100.0
    } else {
        0.0
    };

    println!("\n  {:>20} {:>20}", "Improvement", "");
    println!("  {:->20} {:->20}", "", "");
    row("Optimal rate", format!("{optimal_imp:+.1}%"), "".into());
    row("Convergence speed", format!("{convergence_imp:+.1}%"), "".into());
}

fn main() {
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║  MAB Learning: Linear vs Log+SLO Reward Pipeline               ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Configuration:");
    println!("    Peers: {N_PEERS}");
    println!("    Rounds: {N_ROUNDS}");
    println!("    Warmup: {WARMUP_SAMPLES} samples");
    println!();

    let old_config = rhiza_tuner::RewardConfig {
        slo_latency_us: u64::MAX,
        slo_exponential_factor: 1.0,
        lambda_slo: 0.0,
        latency_base_us: 1_000_000.0,
        ..Default::default()
    };
    let new_config = rhiza_tuner::RewardConfig {
        slo_latency_us: 50_000,
        ..Default::default()
    };

    // Scenario 1: Static optimal (peer-0 is always fastest)
    {
        let nodes = vec![
            SimulatedNode::new("peer-0", 500, 0.0),   // optimal
            SimulatedNode::new("peer-1", 1000, 0.0),
            SimulatedNode::new("peer-2", 1500, 0.0),
            SimulatedNode::new("peer-3", 2000, 0.0),
            SimulatedNode::new("peer-4", 2500, 0.0),
        ];

        let old = run_learning_scenario("Linear", "Static optimal", &nodes, 0, old_config.clone());
        let new = run_learning_scenario("Log+SLO", "Static optimal", &nodes, 0, new_config.clone());
        print_comparison("1. Static optimal (peer-0 always fastest)", &old, &new);
    }

    // Scenario 2: Degraded primary (peer-0 slow but not down)
    {
        let nodes = vec![
            SimulatedNode::new("peer-0", 2000, 0.0),  // degraded (was 500)
            SimulatedNode::new("peer-1", 1000, 0.0),  // now optimal
            SimulatedNode::new("peer-2", 1500, 0.0),
            SimulatedNode::new("peer-3", 2000, 0.0),
            SimulatedNode::new("peer-4", 2500, 0.0),
        ];

        let old = run_learning_scenario("Linear", "Degraded primary", &nodes, 1, old_config.clone());
        let new = run_learning_scenario("Log+SLO", "Degraded primary", &nodes, 1, new_config.clone());
        print_comparison("2. Degraded primary (peer-0 slow, peer-1 optimal)", &old, &new);
    }

    // Scenario 3: Flaky primary (peer-0 has 30% failure rate)
    {
        let nodes = vec![
            SimulatedNode::new("peer-0", 500, 0.3),   // flaky
            SimulatedNode::new("peer-1", 1000, 0.0),  // stable
            SimulatedNode::new("peer-2", 1500, 0.0),
            SimulatedNode::new("peer-3", 2000, 0.0),
            SimulatedNode::new("peer-4", 2500, 0.0),
        ];

        let old = run_learning_scenario("Linear", "Flaky primary", &nodes, 1, old_config.clone());
        let new = run_learning_scenario("Log+SLO", "Flaky primary", &nodes, 1, new_config.clone());
        print_comparison("3. Flaky primary (peer-0 30% failure, peer-1 stable)", &old, &new);
    }

    // Scenario 4: Topology change mid-simulation
    // First 1000 rounds: peer-0 optimal
    // Last 1000 rounds: peer-1 optimal (peer-0 degrades)
    {
        println!("\n{}", "=".repeat(70));
        println!("  4. Topology change (peer-0 optimal → peer-1 optimal)");
        println!("{}", "=".repeat(70));

        let nodes_phase1 = vec![
            SimulatedNode::new("peer-0", 500, 0.0),
            SimulatedNode::new("peer-1", 1000, 0.0),
            SimulatedNode::new("peer-2", 1500, 0.0),
            SimulatedNode::new("peer-3", 2000, 0.0),
            SimulatedNode::new("peer-4", 2500, 0.0),
        ];

        let nodes_phase2 = vec![
            SimulatedNode::new("peer-0", 2000, 0.0),  // degraded
            SimulatedNode::new("peer-1", 500, 0.0),   // now optimal
            SimulatedNode::new("peer-2", 1500, 0.0),
            SimulatedNode::new("peer-3", 2000, 0.0),
            SimulatedNode::new("peer-4", 2500, 0.0),
        ];

        // Run with old config
        let old_result = run_topology_change_scenario("Linear", &nodes_phase1, &nodes_phase2, old_config.clone());

        // Run with new config
        let new_result = run_topology_change_scenario("Log+SLO", &nodes_phase1, &nodes_phase2, new_config.clone());

        println!("\n  {:>20} {:>20} {:>20}", "", "Linear (old)", "Log+SLO (new)");
        println!("  {:->20} {:->20} {:->20}", "", "", "");

        let row = |label: &str, ov: String, nv: String| {
            println!("  {:>20} {:>20} {:>20}", label, ov, nv);
        };

        row("Phase1 optimal",
            format!("{} ({:.1}%)", old_result.phase1_optimal, old_result.phase1_optimal_rate * 100.0),
            format!("{} ({:.1}%)", new_result.phase1_optimal, new_result.phase1_optimal_rate * 100.0));
        row("Phase2 optimal",
            format!("{} ({:.1}%)", old_result.phase2_optimal, old_result.phase2_optimal_rate * 100.0),
            format!("{} ({:.1}%)", new_result.phase2_optimal, new_result.phase2_optimal_rate * 100.0));
        row("Adaptation speed",
            format!("{} rounds", old_result.adaptation_speed),
            format!("{} rounds", new_result.adaptation_speed));
        row("Overall optimal",
            format!("{} ({:.1}%)", old_result.total_optimal, old_result.total_optimal_rate * 100.0),
            format!("{} ({:.1}%)", new_result.total_optimal, new_result.total_optimal_rate * 100.0));
    }

    println!("\n{}", "=".repeat(70));
    println!("  LEARNING SIMULATION COMPLETE");
    println!("{}", "=".repeat(70));
}

struct TopologyChangeResult {
    phase1_optimal: usize,
    phase1_optimal_rate: f64,
    phase2_optimal: usize,
    phase2_optimal_rate: f64,
    adaptation_speed: usize,
    total_optimal: usize,
    total_optimal_rate: f64,
}

fn run_topology_change_scenario(
    pipeline_name: &'static str,
    nodes_phase1: &[SimulatedNode],
    nodes_phase2: &[SimulatedNode],
    reward_config: rhiza_tuner::RewardConfig,
) -> TopologyChangeResult {
    let tuner_config = TunerConfig {
        cold_start_min_samples: WARMUP_SAMPLES as u64,
        ..Default::default()
    };
    let tuner = MabTuner::with_stage(tuner_config, RolloutStage::DefaultOn);
    let identity = test_identity();
    let names = peer_names();
    let candidates = candidate_set();

    let phase1_rounds = N_ROUNDS / 2;
    let phase2_rounds = N_ROUNDS - phase1_rounds;

    // Warmup with phase1 nodes
    for round in 0..WARMUP_SAMPLES {
        for (idx, node) in nodes_phase1.iter().enumerate() {
            let (lat, timeout) = node.sample(round);
            tuner
                .collector()
                .record_proposer_latency(&names[idx], lat, !timeout, timeout);
        }
    }

    let mut phase1_optimal = 0usize;
    let mut phase2_optimal = 0usize;
    let mut adaptation_start = 0usize;
    let mut adaptation_speed = 0usize;

    for round in 0..N_ROUNDS {
        let cid = format!("{pipeline_name}-{round}");
        let result = tuner.select_action(&identity, &names, &candidates, &cid);

        let selected = names
            .iter()
            .position(|n| *n == result.output.action.first_request_target)
            .unwrap_or(0);

        // Select nodes based on phase
        let nodes = if round < phase1_rounds {
            nodes_phase1
        } else {
            nodes_phase2
        };

        let optimal_idx = if round < phase1_rounds { 0 } else { 1 };

        let (lat, timeout) = nodes[selected].sample(round);
        let outcome = if timeout {
            TerminalOutcome::Timeout
        } else {
            TerminalOutcome::Success {
                decision_latency_us: lat,
                additional_rpcs: 0,
                duplicate_proposer_work: false,
                contention_or_round_escalation: false,
            }
        };

        tuner.record_outcome(&cid, &result.output.action, &outcome);
        tuner
            .collector()
            .record_proposer_latency(&names[selected], lat, !timeout, timeout);

        // Track optimal picks per phase
        if round < phase1_rounds {
            if selected == 0 {
                phase1_optimal += 1;
            }
        } else {
            if selected == 1 {
                phase2_optimal += 1;
            }

            // Track adaptation speed (first time picking peer-1 after phase change)
            if selected == 1 && adaptation_start == 0 {
                adaptation_start = round;
                adaptation_speed = round - phase1_rounds;
            }
        }
    }

    let total_optimal = phase1_optimal + phase2_optimal;

    TopologyChangeResult {
        phase1_optimal,
        phase1_optimal_rate: phase1_optimal as f64 / phase1_rounds as f64,
        phase2_optimal,
        phase2_optimal_rate: phase2_optimal as f64 / phase2_rounds as f64,
        adaptation_speed,
        total_optimal,
        total_optimal_rate: total_optimal as f64 / N_ROUNDS as f64,
    }
}
