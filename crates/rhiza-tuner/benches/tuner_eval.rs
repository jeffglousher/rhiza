//! Performance evaluation benchmarks for rhiza-tuner.
//!
//! Measures:
//! - select_action latency overhead
//! - record_outcome latency overhead
//! - TelemetryCollector feature assembly overhead
//! - Bandit convergence quality under simulated workload
//! - Memory usage scaling

use std::time::{Duration, Instant};

use rhiza_tuner::collector::{CollectorConfig, TelemetryCollector};
use rhiza_tuner::reward::{RewardConfig, RewardPipeline};
use rhiza_tuner::types::*;
use rhiza_tuner::{MabTuner, RolloutStage, TunerConfig};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_identity() -> Identity {
    Identity {
        cluster_id: "bench-cluster".into(),
        epoch: 1,
        config_id: 1,
        membership_digest: [0u8; 32],
        recovery_generation: 0,
    }
}

fn proposers(n: usize) -> Vec<NodeId> {
    (0..n).map(|i| format!("node-{i}")).collect()
}

fn candidate_set(n_voters: usize) -> CandidateSet {
    CandidateSet {
        identity: test_identity(),
        eligible_voters: proposers(n_voters),
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

fn default_config() -> TunerConfig {
    TunerConfig {
        cold_start_min_samples: 0,
        ..Default::default()
    }
}

fn warm_collector(n_samples: u64, n_proposers: usize) -> TelemetryCollector {
    let collector = TelemetryCollector::new(CollectorConfig {
        min_sample_count: n_samples,
        ..Default::default()
    });
    let p = proposers(n_proposers);
    for i in 0..n_samples {
        let pid = &p[i as usize % n_proposers];
        collector.record_proposer_latency(pid, 1000 + (i % 500), true, false);
    }
    collector
}

// ---------------------------------------------------------------------------
// Benchmark 1: select_action latency (cold)
// ---------------------------------------------------------------------------

fn bench_select_action_cold(n_proposers: usize, iterations: usize) -> Vec<Duration> {
    let tuner = MabTuner::with_stage(default_config(), RolloutStage::DefaultOn);
    let identity = test_identity();
    let proposers = proposers(n_proposers);
    let candidates = candidate_set(n_proposers);

    let mut durations = Vec::with_capacity(iterations);
    for i in 0..iterations {
        let cid = format!("req-{i}");
        let start = Instant::now();
        let _result = tuner.select_action(&identity, &proposers, &candidates, &cid);
        durations.push(start.elapsed());
    }
    durations
}

// ---------------------------------------------------------------------------
// Benchmark 2: select_action latency (warm / post-cold-start)
// ---------------------------------------------------------------------------

fn bench_select_action_warm(n_proposers: usize, iterations: usize) -> Vec<Duration> {
    let config = TunerConfig {
        cold_start_min_samples: 100,
        ..Default::default()
    };
    let tuner = MabTuner::with_stage(config, RolloutStage::DefaultOn);
    let identity = test_identity();
    let proposers_vec = proposers(n_proposers);
    let candidates = candidate_set(n_proposers);

    // Warm up with enough samples to pass cold-start gate
    for i in 0..100u64 {
        let pid = &proposers_vec[i as usize % n_proposers];
        tuner
            .collector()
            .record_proposer_latency(pid, 1000 + (i % 500), true, false);
    }

    let mut durations = Vec::with_capacity(iterations);
    for i in 0..iterations {
        let cid = format!("req-{i}");
        let start = Instant::now();
        let _result = tuner.select_action(&identity, &proposers_vec, &candidates, &cid);
        durations.push(start.elapsed());
    }
    durations
}

// ---------------------------------------------------------------------------
// Benchmark 3: record_outcome latency
// ---------------------------------------------------------------------------

fn bench_record_outcome(iterations: usize) -> Vec<Duration> {
    let tuner = MabTuner::with_stage(default_config(), RolloutStage::DefaultOn);
    let identity = test_identity();
    let proposers_vec = proposers(3);
    let candidates = candidate_set(3);

    // Generate some actions first
    let mut actions = Vec::with_capacity(iterations);
    for i in 0..iterations {
        let cid = format!("req-{i}");
        let result = tuner.select_action(&identity, &proposers_vec, &candidates, &cid);
        actions.push((cid, result.output.action.clone()));
    }

    let mut durations = Vec::with_capacity(iterations);
    for (cid, action) in &actions {
        let outcome = TerminalOutcome::Success {
            decision_latency_us: 5000,
            additional_rpcs: 1,
            duplicate_proposer_work: false,
            contention_or_round_escalation: false,
        };
        let start = Instant::now();
        tuner.record_outcome(cid, action, &outcome);
        durations.push(start.elapsed());
    }
    durations
}

// ---------------------------------------------------------------------------
// Benchmark 4: TelemetryCollector feature assembly
// ---------------------------------------------------------------------------

fn bench_feature_assembly(n_proposers: usize, iterations: usize) -> Vec<Duration> {
    let collector = warm_collector(1000, n_proposers);
    let identity = test_identity();
    let proposers_vec = proposers(n_proposers);

    let mut durations = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let _features = collector.assemble_features(identity.clone(), &proposers_vec);
        durations.push(start.elapsed());
    }
    durations
}

// ---------------------------------------------------------------------------
// Benchmark 5: Bandit convergence quality
// ---------------------------------------------------------------------------

fn bench_bandit_convergence(n_proposers: usize, n_rounds: usize) -> BanditConvergenceResult {
    let config = TunerConfig {
        cold_start_min_samples: 0,
        ..Default::default()
    };
    let tuner = MabTuner::with_stage(config, RolloutStage::DefaultOn);
    let identity = test_identity();
    let proposers_vec = proposers(n_proposers);
    let candidates = candidate_set(n_proposers);

    // Simulate: proposer 0 is fastest (1ms), others are slower (10-50ms)
    let base_latencies: Vec<u64> = (0..n_proposers)
        .map(|i| {
            if i == 0 {
                1000
            } else {
                10000 + (i as u64 * 10000)
            }
        })
        .collect();

    let mut preferred_selections = 0;
    let mut total_reward = 0.0;
    let mut exploration_count = 0;

    for i in 0..n_rounds {
        let cid = format!("round-{i}");
        let result = tuner.select_action(&identity, &proposers_vec, &candidates, &cid);

        if result.output.exploration {
            exploration_count += 1;
        }

        // Check if the tuner selected the optimal proposer
        let selected_idx = proposers_vec
            .iter()
            .position(|p| *p == result.output.action.first_request_target)
            .unwrap_or(0);
        if selected_idx == 0 {
            preferred_selections += 1;
        }

        // Simulate outcome based on selected proposer's latency
        let latency = base_latencies[selected_idx];
        let outcome = TerminalOutcome::Success {
            decision_latency_us: latency,
            additional_rpcs: if selected_idx > 0 { 1 } else { 0 },
            duplicate_proposer_work: selected_idx > 0,
            contention_or_round_escalation: false,
        };
        tuner.record_outcome(&cid, &result.output.action, &outcome);

        let mut reward_pipeline = RewardPipeline::new(RewardConfig::default());
        let reward = reward_pipeline.compute(&outcome);
        total_reward += reward.total;
    }

    BanditConvergenceResult {
        n_proposers,
        n_rounds,
        preferred_selections,
        preferred_rate: preferred_selections as f64 / n_rounds as f64,
        exploration_count,
        exploration_rate: exploration_count as f64 / n_rounds as f64,
        mean_reward: total_reward / n_rounds as f64,
    }
}

struct BanditConvergenceResult {
    n_proposers: usize,
    n_rounds: usize,
    preferred_selections: usize,
    preferred_rate: f64,
    exploration_count: usize,
    exploration_rate: f64,
    mean_reward: f64,
}

impl std::fmt::Display for BanditConvergenceResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "  proposers:       {}", self.n_proposers)?;
        writeln!(f, "  rounds:          {}", self.n_rounds)?;
        writeln!(
            f,
            "  preferred picks: {} ({:.1}%)",
            self.preferred_selections,
            self.preferred_rate * 100.0
        )?;
        writeln!(
            f,
            "  exploration:     {} ({:.1}%)",
            self.exploration_count,
            self.exploration_rate * 100.0
        )?;
        writeln!(f, "  mean reward:     {:.4}", self.mean_reward)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Benchmark 6: Kill switch overhead
// ---------------------------------------------------------------------------

fn bench_kill_switch_active(n_proposers: usize, iterations: usize) -> Vec<Duration> {
    let tuner = MabTuner::with_stage(default_config(), RolloutStage::DefaultOn);
    tuner.activate_kill_switch("bench");
    let identity = test_identity();
    let proposers_vec = proposers(n_proposers);
    let candidates = candidate_set(n_proposers);

    let mut durations = Vec::with_capacity(iterations);
    for i in 0..iterations {
        let cid = format!("req-{i}");
        let start = Instant::now();
        let _result = tuner.select_action(&identity, &proposers_vec, &candidates, &cid);
        durations.push(start.elapsed());
    }
    durations
}

// ---------------------------------------------------------------------------
// Statistics helper
// ---------------------------------------------------------------------------

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn stats(durations: &[Duration]) -> DurationStats {
    let mut sorted = durations.to_vec();
    sorted.sort();
    let sum: Duration = sorted.iter().sum();
    DurationStats {
        count: sorted.len(),
        min: sorted[0],
        p50: percentile(&sorted, 50.0),
        p95: percentile(&sorted, 95.0),
        p99: percentile(&sorted, 99.0),
        max: *sorted.last().unwrap(),
        mean: sum / sorted.len() as u32,
    }
}

struct DurationStats {
    count: usize,
    min: Duration,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    max: Duration,
    mean: Duration,
}

impl std::fmt::Display for DurationStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "  count:  {}", self.count)?;
        writeln!(f, "  min:    {:?}", self.min)?;
        writeln!(f, "  p50:    {:?}", self.p50)?;
        writeln!(f, "  p95:    {:?}", self.p95)?;
        writeln!(f, "  p99:    {:?}", self.p99)?;
        writeln!(f, "  max:    {:?}", self.max)?;
        writeln!(f, "  mean:   {:?}", self.mean)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Main evaluation
// ---------------------------------------------------------------------------

fn main() {
    let iterations = 10_000;
    let n_proposers = 3;

    println!("=== rhiza-tuner Performance Evaluation ===\n");

    // 1. select_action cold
    println!("1. select_action (cold start, no prior data):");
    let cold = bench_select_action_cold(n_proposers, iterations);
    println!("{}", stats(&cold));

    // 2. select_action warm
    println!("2. select_action (warm, post-cold-start):");
    let warm = bench_select_action_warm(n_proposers, iterations);
    println!("{}", stats(&warm));

    // 3. record_outcome
    println!("3. record_outcome:");
    let outcome = bench_record_outcome(iterations);
    println!("{}", stats(&outcome));

    // 4. feature assembly
    println!("4. TelemetryCollector.assemble_features (3 proposers):");
    let fa3 = bench_feature_assembly(3, iterations);
    println!("{}", stats(&fa3));

    println!("5. TelemetryCollector.assemble_features (7 proposers):");
    let fa7 = bench_feature_assembly(7, iterations);
    println!("{}", stats(&fa7));

    // 5. Bandit convergence
    println!("6. Bandit convergence (3 proposers, proposer-0 is optimal):");
    let conv3 = bench_bandit_convergence(3, 1000);
    println!("{}", conv3);

    println!("7. Bandit convergence (7 proposers, proposer-0 is optimal):");
    let conv7 = bench_bandit_convergence(7, 1000);
    println!("{}", conv7);

    // 6. Kill switch
    println!("8. select_action (kill switch active):");
    let killed = bench_kill_switch_active(n_proposers, iterations);
    println!("{}", stats(&killed));

    // 7. Scaling test
    println!("9. select_action scaling (varying proposer count):");
    for n in [3, 5, 7] {
        let durations = bench_select_action_warm(n, 1000);
        let s = stats(&durations);
        println!("  {n} proposers: p50={:?} p99={:?}", s.p50, s.p99);
    }

    println!("\n=== Evaluation Complete ===");
}
