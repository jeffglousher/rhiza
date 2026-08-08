//! 5-peer cluster benchmark using actual rhiza client behavior.
//!
//! Client perspective (correct model):
//! 1. Send HTTP request to preferred endpoint first
//! 2. After hedge_delay, hedge to next endpoint if no response
//! 3. Each node internally runs QuePaxa (transparent to client)
//!
//! Tuner's role: choose which endpoint is first + hedge delay

use rhiza_tuner::types::*;
use rhiza_tuner::{MabTuner, RolloutStage, TunerConfig};

const N_PEERS: usize = 5;
const N_ROUNDS: usize = 1000;

// ---------------------------------------------------------------------------
// Node response time simulation (client perspective)
// ---------------------------------------------------------------------------

struct NodeResponseTime {
    /// Response time when node is healthy (µs)
    healthy_us: u64,
    /// Response time when node is degraded (µs)
    degraded_us: u64,
    /// Probability of timeout
    timeout_rate: f64,
    /// Current state
    state: NodeState,
}

#[derive(Clone, Copy, PartialEq)]
enum NodeState {
    Healthy,
    Degraded,
    Down,
}

impl NodeResponseTime {
    fn healthy(us: u64) -> Self {
        Self {
            healthy_us: us,
            degraded_us: us * 3,
            timeout_rate: 0.0,
            state: NodeState::Healthy,
        }
    }

    fn with_state(mut self, state: NodeState) -> Self {
        self.state = state;
        self
    }

    fn sample(&self, round: u64) -> (u64, bool) {
        let jitter = ((round * 7 + 13) % 200) as i64 - 100;
        let base = match self.state {
            NodeState::Healthy => self.healthy_us,
            NodeState::Degraded => self.degraded_us,
            NodeState::Down => 100_000, // will timeout
        };
        let latency = (base as i64 + jitter).max(100) as u64;

        // Deterministic timeout based on round and rate
        let timeout_check = (round * 31 % 1000) as f64 / 1000.0;
        let timeout = match self.state {
            NodeState::Down => true,
            NodeState::Degraded => timeout_check < self.timeout_rate,
            NodeState::Healthy => false,
        };

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
        cluster_id: "bench-cluster".into(),
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

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 * p / 100.0) as usize).min(sorted.len() - 1);
    sorted[idx]
}

// ---------------------------------------------------------------------------
// Client request simulation (correct model)
// ---------------------------------------------------------------------------

/// Simulate one client write request.
///
/// Client behavior:
/// 1. Send to preferred endpoint
/// 2. If timeout/error, wait hedge_delay then try next
/// 3. Continue until success or all endpoints exhausted
///
/// Returns (total_latency_us, success, endpoints_tried)
fn simulate_client_request(
    preferred: usize,
    nodes: &[NodeResponseTime],
    hedge_delay_us: u64,
    attempt_timeout_us: u64,
    round: u64,
) -> (u64, bool, u32) {
    let mut tried = 0u32;
    // Try preferred first
    let (lat, timeout) = nodes[preferred].sample(round);
    tried += 1;

    if !timeout && lat < attempt_timeout_us {
        // Success on first try
        return (lat, true, tried);
    }

    // Preferred failed/timed out. Hedge to others after hedge_delay.
    let mut elapsed = if timeout { attempt_timeout_us } else { lat };

    for (next, node) in nodes.iter().enumerate() {
        if next == preferred {
            continue;
        }

        // Wait for hedge delay
        elapsed += hedge_delay_us;

        let (lat, timeout) = node.sample(round);
        tried += 1;

        if !timeout && lat < attempt_timeout_us {
            // Success
            return (elapsed + lat, true, tried);
        }

        elapsed += if timeout { attempt_timeout_us } else { lat };
    }

    // All failed
    (elapsed, false, tried)
}

// ---------------------------------------------------------------------------
// Scenario runner
// ---------------------------------------------------------------------------

struct ScenarioResult {
    policy: String,
    rounds: usize,
    successes: usize,
    timeouts: usize,
    p50_latency_us: u64,
    p95_latency_us: u64,
    p99_latency_us: u64,
    mean_tried: f64,
}

impl std::fmt::Display for ScenarioResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "  Policy:           {}", self.policy)?;
        writeln!(
            f,
            "  Success/Timeout:  {}/{} ({:.1}%)",
            self.successes,
            self.timeouts,
            self.successes as f64 / self.rounds as f64 * 100.0
        )?;
        writeln!(f, "  p50 latency:      {} µs", self.p50_latency_us)?;
        writeln!(f, "  p95 latency:      {} µs", self.p95_latency_us)?;
        writeln!(f, "  p99 latency:      {} µs", self.p99_latency_us)?;
        writeln!(f, "  Mean tried:       {:.2}", self.mean_tried)?;
        Ok(())
    }
}

fn run_static_scenario(
    _label: &'static str,
    nodes: &[NodeResponseTime],
    preferred: usize,
    hedge_delay_us: u64,
) -> ScenarioResult {
    let attempt_timeout = 50_000; // 50ms attempt timeout
    let mut latencies = Vec::with_capacity(N_ROUNDS);
    let mut successes = 0usize;
    let mut timeouts = 0usize;
    let mut total_tried = 0u32;

    for round in 0..N_ROUNDS {
        let (lat, success, tried) = simulate_client_request(
            preferred,
            nodes,
            hedge_delay_us,
            attempt_timeout,
            round as u64,
        );
        if success {
            successes += 1;
        } else {
            timeouts += 1;
        }
        latencies.push(lat);
        total_tried += tried;
    }

    latencies.sort();
    ScenarioResult {
        policy: format!("static (peer-{preferred})"),
        rounds: N_ROUNDS,
        successes,
        timeouts,
        p50_latency_us: percentile(&latencies, 50.0),
        p95_latency_us: percentile(&latencies, 95.0),
        p99_latency_us: percentile(&latencies, 99.0),
        mean_tried: total_tried as f64 / N_ROUNDS as f64,
    }
}

fn run_tuner_scenario(
    label: &'static str,
    nodes: &[NodeResponseTime],
    warmup_rounds: usize,
) -> ScenarioResult {
    let attempt_timeout = 50_000; // 50ms attempt timeout
    let config = TunerConfig {
        cold_start_min_samples: warmup_rounds as u64,
        ..Default::default()
    };
    let tuner = MabTuner::with_stage(config, RolloutStage::DefaultOn);
    let identity = test_identity();
    let names = peer_names();
    let candidates = candidate_set();

    // Warmup: feed telemetry from all nodes
    for round in 0..warmup_rounds {
        for (idx, node) in nodes.iter().enumerate() {
            let (lat, timeout) = node.sample(round as u64);
            tuner
                .collector()
                .record_proposer_latency(&names[idx], lat, !timeout, timeout);
        }
    }

    let mut latencies = Vec::with_capacity(N_ROUNDS);
    let mut successes = 0usize;
    let mut timeouts = 0usize;
    let mut total_tried = 0u32;

    for round in 0..N_ROUNDS {
        let cid = format!("{label}-{round}");
        let result = tuner.select_action(&identity, &names, &candidates, &cid);

        let selected = names
            .iter()
            .position(|n| *n == result.output.action.first_request_target)
            .unwrap_or(0);

        let hedge_delay_ms = result.output.action.hedge_delay.as_ms().unwrap_or(100);
        let hedge_delay_us = hedge_delay_ms * 1000;

        let (lat, success, tried) = simulate_client_request(
            selected,
            nodes,
            hedge_delay_us,
            attempt_timeout,
            round as u64,
        );

        if success {
            successes += 1;
        } else {
            timeouts += 1;
        }
        latencies.push(lat);
        total_tried += tried;

        // Record outcome for tuner learning
        let outcome = if success {
            TerminalOutcome::Success {
                decision_latency_us: lat,
                additional_rpcs: tried - 1,
                duplicate_proposer_work: tried > 1,
                contention_or_round_escalation: false,
            }
        } else {
            TerminalOutcome::Timeout
        };
        tuner.record_outcome(&cid, &result.output.action, &outcome);

        // Feed telemetry
        tuner
            .collector()
            .record_proposer_latency(&names[selected], lat, success, !success);
    }

    latencies.sort();
    ScenarioResult {
        policy: "MAB tuner".to_string(),
        rounds: N_ROUNDS,
        successes,
        timeouts,
        p50_latency_us: percentile(&latencies, 50.0),
        p95_latency_us: percentile(&latencies, 95.0),
        p99_latency_us: percentile(&latencies, 99.0),
        mean_tried: total_tried as f64 / N_ROUNDS as f64,
    }
}

fn print_comparison(scenario: &str, s: &ScenarioResult, t: &ScenarioResult) {
    println!("\n{}", "=".repeat(70));
    println!("  {scenario}");
    println!("{}", "=".repeat(70));

    println!("\n  {:>20} {:>20} {:>20}", "", "Static", "MAB Tuner");
    println!("  {:->20} {:->20} {:->20}", "", "", "");

    let row = |label: &str, sv: String, tv: String| {
        println!("  {:>20} {:>20} {:>20}", label, sv, tv);
    };

    row(
        "Success rate",
        format!("{:.1}%", s.successes as f64 / s.rounds as f64 * 100.0),
        format!("{:.1}%", t.successes as f64 / t.rounds as f64 * 100.0),
    );
    row(
        "Timeouts",
        format!("{}", s.timeouts),
        format!("{}", t.timeouts),
    );
    row(
        "p50 latency",
        format!("{} µs", s.p50_latency_us),
        format!("{} µs", t.p50_latency_us),
    );
    row(
        "p95 latency",
        format!("{} µs", s.p95_latency_us),
        format!("{} µs", t.p95_latency_us),
    );
    row(
        "p99 latency",
        format!("{} µs", s.p99_latency_us),
        format!("{} µs", t.p99_latency_us),
    );
    row(
        "Mean tried",
        format!("{:.2}", s.mean_tried),
        format!("{:.2}", t.mean_tried),
    );

    // Improvement
    let p50_imp = if s.p50_latency_us > 0 {
        (s.p50_latency_us as f64 - t.p50_latency_us as f64) / s.p50_latency_us as f64 * 100.0
    } else {
        0.0
    };
    let p99_imp = if s.p99_latency_us > 0 {
        (s.p99_latency_us as f64 - t.p99_latency_us as f64) / s.p99_latency_us as f64 * 100.0
    } else {
        0.0
    };

    println!("\n  {:>20} {:>20}", "p50 improvement", "");
    println!("  {:->20} {:->20}", "", "");
    row("", format!("{p50_imp:+.1}%"), "".into());
    row("p99 improvement", format!("{p99_imp:+.1}%"), "".into());
}

fn main() {
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║  5-Peer Cluster: Static vs MAB Tuner (Client Perspective)      ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Client behavior:");
    println!("    - Send HTTP request to preferred endpoint first");
    println!("    - After hedge_delay, hedge to next endpoint");
    println!("    - Attempt timeout: 50ms");
    println!("    - Static hedge delay: 100ms");
    println!();
    println!("  Config: {N_PEERS} peers, {N_ROUNDS} rounds, 200 warmup samples");

    // Scenario 1: All healthy, peer-0 optimal
    {
        let nodes = [
            NodeResponseTime::healthy(500),
            NodeResponseTime::healthy(1000),
            NodeResponseTime::healthy(1500),
            NodeResponseTime::healthy(2000),
            NodeResponseTime::healthy(2500),
        ];
        let s = run_static_scenario("all-healthy", &nodes, 0, 100_000);
        let t = run_tuner_scenario("all-healthy", &nodes, 200);
        print_comparison("1. All healthy (peer-0 fastest)", &s, &t);
    }

    // Scenario 2: Primary degraded (2x latency)
    {
        let nodes = [
            NodeResponseTime::healthy(1000).with_state(NodeState::Degraded),
            NodeResponseTime::healthy(1000),
            NodeResponseTime::healthy(1500),
            NodeResponseTime::healthy(2000),
            NodeResponseTime::healthy(2500),
        ];
        let s = run_static_scenario("primary-slow", &nodes, 0, 100_000);
        let t = run_tuner_scenario("primary-slow", &nodes, 200);
        print_comparison("2. Primary degraded (2x latency)", &s, &t);
    }

    // Scenario 3: Primary down
    {
        let nodes = [
            NodeResponseTime::healthy(500).with_state(NodeState::Down),
            NodeResponseTime::healthy(1000),
            NodeResponseTime::healthy(1500),
            NodeResponseTime::healthy(2000),
            NodeResponseTime::healthy(2500),
        ];
        let s = run_static_scenario("primary-down", &nodes, 0, 100_000);
        let t = run_tuner_scenario("primary-down", &nodes, 200);
        print_comparison("3. Primary DOWN (timeout → hedge)", &s, &t);
    }

    // Scenario 4: Primary flaky (50% timeout)
    {
        let nodes = [
            NodeResponseTime::healthy(500),
            NodeResponseTime::healthy(1000),
            NodeResponseTime::healthy(1500),
            NodeResponseTime::healthy(2000),
            NodeResponseTime::healthy(2500),
        ];
        // Set peer-0 to have 50% timeout
        let mut nodes = nodes;
        nodes[0].timeout_rate = 0.5;
        nodes[0].state = NodeState::Degraded;

        let s = run_static_scenario("primary-flaky", &nodes, 0, 100_000);
        let t = run_tuner_scenario("primary-flaky", &nodes, 200);
        print_comparison("4. Primary flaky (50% timeout)", &s, &t);
    }

    // Scenario 5: Cascading failure
    {
        let nodes = [
            NodeResponseTime::healthy(500).with_state(NodeState::Down),
            NodeResponseTime::healthy(3000).with_state(NodeState::Degraded),
            NodeResponseTime::healthy(1500),
            NodeResponseTime::healthy(2000),
            NodeResponseTime::healthy(2500),
        ];
        let s = run_static_scenario("cascading", &nodes, 0, 100_000);
        let t = run_tuner_scenario("cascading", &nodes, 200);
        print_comparison("5. Cascading: peer-0 down, peer-1 slow", &s, &t);
    }

    // Scenario 6: Recovery
    {
        let down_nodes = [
            NodeResponseTime::healthy(500).with_state(NodeState::Down),
            NodeResponseTime::healthy(1000),
            NodeResponseTime::healthy(1500),
            NodeResponseTime::healthy(2000),
            NodeResponseTime::healthy(2500),
        ];
        let healthy_nodes = [
            NodeResponseTime::healthy(500),
            NodeResponseTime::healthy(1000),
            NodeResponseTime::healthy(1500),
            NodeResponseTime::healthy(2000),
            NodeResponseTime::healthy(2500),
        ];

        // Static: half down, half healthy
        let mut static_lats = Vec::with_capacity(N_ROUNDS);
        let mut static_success = 0usize;
        let mut static_timeout = 0usize;
        let mut static_tried = 0u32;
        for round in 0..N_ROUNDS {
            let nodes = if round < N_ROUNDS / 2 {
                &down_nodes
            } else {
                &healthy_nodes
            };
            let (lat, success, tried) =
                simulate_client_request(0, nodes, 100_000, 50_000, round as u64);
            if success {
                static_success += 1;
            } else {
                static_timeout += 1;
            }
            static_lats.push(lat);
            static_tried += tried;
        }
        static_lats.sort();

        // Tuner
        let tuner = MabTuner::with_stage(
            TunerConfig {
                cold_start_min_samples: 200,
                ..Default::default()
            },
            RolloutStage::DefaultOn,
        );
        let identity = test_identity();
        let names = peer_names();
        let candidates = candidate_set();

        for round in 0..200 {
            for (idx, node) in down_nodes.iter().enumerate() {
                let (lat, timeout) = node.sample(round);
                tuner
                    .collector()
                    .record_proposer_latency(&names[idx], lat, !timeout, timeout);
            }
        }

        let mut tuner_lats = Vec::with_capacity(N_ROUNDS);
        let mut tuner_success = 0usize;
        let mut tuner_timeout = 0usize;
        let mut tuner_tried = 0u32;
        for round in 0..N_ROUNDS {
            let cid = format!("recovery-{round}");
            let result = tuner.select_action(&identity, &names, &candidates, &cid);
            let selected = names
                .iter()
                .position(|n| *n == result.output.action.first_request_target)
                .unwrap_or(0);
            let hedge_us = result.output.action.hedge_delay.as_ms().unwrap_or(100) * 1000;

            let nodes = if round < N_ROUNDS / 2 {
                &down_nodes
            } else {
                &healthy_nodes
            };
            let (lat, success, tried) =
                simulate_client_request(selected, nodes, hedge_us, 50_000, round as u64);
            if success {
                tuner_success += 1;
            } else {
                tuner_timeout += 1;
            }
            tuner_lats.push(lat);
            tuner_tried += tried;

            let outcome = if success {
                TerminalOutcome::Success {
                    decision_latency_us: lat,
                    additional_rpcs: tried - 1,
                    duplicate_proposer_work: tried > 1,
                    contention_or_round_escalation: false,
                }
            } else {
                TerminalOutcome::Timeout
            };
            tuner.record_outcome(&cid, &result.output.action, &outcome);
            tuner
                .collector()
                .record_proposer_latency(&names[selected], lat, success, !success);
        }
        tuner_lats.sort();

        let s = ScenarioResult {
            policy: "static (peer-0)".into(),
            rounds: N_ROUNDS,
            successes: static_success,
            timeouts: static_timeout,
            p50_latency_us: percentile(&static_lats, 50.0),
            p95_latency_us: percentile(&static_lats, 95.0),
            p99_latency_us: percentile(&static_lats, 99.0),
            mean_tried: static_tried as f64 / N_ROUNDS as f64,
        };
        let t = ScenarioResult {
            policy: "MAB tuner".into(),
            rounds: N_ROUNDS,
            successes: tuner_success,
            timeouts: tuner_timeout,
            p50_latency_us: percentile(&tuner_lats, 50.0),
            p95_latency_us: percentile(&tuner_lats, 95.0),
            p99_latency_us: percentile(&tuner_lats, 99.0),
            mean_tried: tuner_tried as f64 / N_ROUNDS as f64,
        };
        print_comparison("6. Recovery: peer-0 down → healthy", &s, &t);
    }

    println!("\n{}", "=".repeat(70));
    println!("  BENCHMARK COMPLETE");
    println!("{}", "=".repeat(70));
}
