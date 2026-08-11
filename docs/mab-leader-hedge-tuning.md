# Runtime Request-Routing Tuner

> **Status: PHASE 1 IMPLEMENTED, OPT-IN.** `rhiza-client/tuner` can attach a
> trusted routing snapshot and `RoutingTuner` at runtime. The CLI and deployment
> images do not enable it by default. Phase 1 selects only the first healthy
> request endpoint; hedge delay stays fixed at 100 ms.

## Implemented Phase 1

- `RoutingSnapshot` validates a unique `NodeId` ↔ URL mapping, learning identity,
  topology generation, eligibility, and static primary.
- `RoutingTuner::plan` returns the exact endpoint order that the client executes,
  a fixed hedge delay, rollout metadata, selection propensity, and an opaque
  single-use observation ticket.
- Canary assignment is stable for the lifetime of the attached client routing
  cohort; per-request correlation IDs remain unique observation identifiers.
- `RhizaClient` records actual attempt order, timing, timer-hedge launch, winner,
  and terminal outcome. `RoutingTuner::observe` learns only when that trace
  matches the ticket, identity, topology generation, and applied action.
- The model is a small first-target-only UCB policy with EWMA reward. It never
  creates target×delay arms.
- `Disabled`, `Shadow`, `ProposerCanary`, `ProposerDefault`, `HedgeCanary`, and
  `BoundedDefault` are runtime stages. Phase 1 changes routing only in proposer
  stages; the hedge-related stages reserve future rollout names but still use
  the fixed delay.
- The runtime kill switch makes the next plan use validated static routing and
  prevents learning without rebuilding or restarting.
- The older experimental `MabTuner`/`ContextualBandit` API remains only for
  source and benchmark compatibility. It is not connected to `RhizaClient` and
  is outside this phase-one contract; new integrations must use `RoutingTuner`.

## Goals

- Select the preferred proposer that is most likely to complete a write quickly
  under current cluster conditions.
- Preserve the existing fixed hedge policy while first-target tuning is proven.
- Evaluate hedge-delay tuning only as a later, separately controlled phase.
- Adapt to topology, load, and partial degradation while remaining observable,
  reversible, and no worse than a fixed-policy baseline.

## Non-goals

- Changing QuePaxa messages, ballots, priorities, phases, certificates,
  membership, quorum size, or proof validation.
- Electing an authoritative leader, granting leases, or making one proposer
  necessary for progress.
- Predicting application semantics, changing command order, or weakening
  durability and recovery requirements.
- Allowing an online model to emit arbitrary timings or protocol parameters.
- Replacing QuePaxa's current static priority identity (`membership[0]`). The
  first implementation may route a request to an eligible member first, but it
  does not change `ProposalPriority::MAX`. Making that identity tunable is a
  separate protocol change requiring a new safety review and wire contract.

## Safety Boundary

The tuner is an advisory scheduling component outside the QuePaxa state machine.
It may choose which eligible member receives a client request first and when an
additional eligible proposer is started. This request-routing preference is not
the protocol's static priority identity. Every attempt still executes the
ordinary QuePaxa protocol and must produce a normally valid decision certificate.

The tuner must never affect QuePaxa validity, quorum, or liveness correctness:

- Recorder acceptance rules, quorum calculations, proof checks, membership,
  persistent state, and recovery do not consume model output.
- A preferred proposer is only a performance hint; any eligible proposer can use
  the leaderless path and finish an already observed valid proposal.
- Hedging adds an ordinary proposer attempt. It cannot manufacture a decision,
  reduce a quorum, suppress protocol retries, or cancel the baseline path needed
  for progress.
- Missing, stale, invalid, or unavailable model output atomically falls back to
  static proposer selection and a configured static hedge delay.

## Phase 1 Learning Model

Phase 1 uses only prevalidated eligible targets. It maintains one small arm per
`NodeId`, records an EWMA latency reward, explores untried targets only in the
bounded proposer canary, and otherwise uses deterministic UCB selection. The
static target wins cold-start ties. `ProposerDefault` freezes exploration.

The learning identity contains the exact consensus identity tuple plus a routing
policy version. Any identity change clears model and pending-ticket state
atomically. A topology-generation change censors an in-flight observation but
does not discard compatible historical target statistics.

## Future Contextual Signals

Use a contextual bandit with a bounded, versioned feature vector. Model and
policy state is keyed by the exact tuple `(cluster_id, epoch, config_id,
membership_digest, recovery_generation)` and reset whenever any field changes.
Inputs are lagged measurements available before action selection; no
outcome-derived or command-payload data is included.

Features:

- Configuration epoch, voter count, proposer eligibility, and coarse region or
  failure-domain relationships.
- Per-proposer rolling decision-latency quantiles, success/timeout rates,
  in-flight proposals, queue depth, and recent contention rate.
- Aggregated proposer-to-recorder RPC latency, timeout, and error rates, including
  quorum-order statistics rather than raw peer identities where practical.
- Coarse node CPU, I/O pressure, and event-loop or executor delay.
- Request class limited to stable operational buckets such as durability mode and
  size bucket; never SQL text, tenant identity, or command content.
- Feature age, sample count, and missingness flags so stale or sparse telemetry is
  explicit.

### Future Actions

Phase 1 has one action dimension: `first_request_target`. A later hedge phase may
add a separate, bounded delay experiment only after the target policy is frozen.
It must not reintroduce a joint target×delay arm space.

The future delay action, if enabled, is:

1. `hedge_delay`: one administrator-approved bucket between the configured
   nonzero minimum and maximum, for example `5`, `10`, `25`, `50`, or `100 ms`,
   plus `static` to use the configured baseline.

The candidate set is rebuilt for each exact configuration identity. An output
also carries that identity, `valid_from_slot`, expiry, policy/model version,
exploration flag, and confidence. Outputs naming an ineligible member, a delay
outside the configured allowlist, a future slot, an expired validity interval,
or a mismatched identity are rejected. Selection is fixed for one request; the
model cannot alter an in-flight attempt.

### Reward and Cost

Score only requests with a terminal, attributable observation. A proposed
dimensionless reward is:

```text
reward = -normalized(decision_latency)
         - lambda_rpc * additional_rpc_count
         - lambda_work * duplicate_proposer_work
         - lambda_contend * contention_or_round_escalation
         - lambda_error * terminal_error
```

Normalization and coefficients are versioned configuration. Latency is capped
for training robustness, while timeout and terminal-error penalties remain
explicit. Cancelled, reconfigured, or telemetry-incomplete requests are recorded
as censored outcomes. Training must use an explicit censoring strategy or a
conservative penalty and report their rate; silently excluding them is forbidden
because it introduces survivorship bias.

### Exploration and Cold Start

- Start with the static policy until telemetry and attribution gates pass.
- Begin learning with a deterministic, small proposer-canary cohort; static-only
  traffic is not expected to produce samples for untried targets.
- Exploration chooses only validated eligible endpoints.
- Cap exploratory traffic per cluster and per time window; disable exploration
  automatically during incidents, reconfiguration, overload, or elevated errors.
- Initialize new epochs and newly eligible proposers from the static policy.
  Historical priors may transfer only across explicitly compatible topology and
  software-version buckets, with reduced confidence.

## Bounds and Fallback

- Clamp hedge delay to administrator-set minimum and maximum values and the
  discrete allowlist. A nonzero minimum may be required to bound duplicate load.
- Phase 1 preserves the client's existing bounded progression through its finite
  configured endpoint set at fixed hedge intervals. The tuner neither adds
  attempts nor changes the interval. Any future stricter hedge budget is a
  separate transport-policy change.
- Refuse decisions when features exceed a freshness limit, model/config versions
  disagree, confidence is below threshold, or guardrail metrics are unhealthy.
- Keep a locally cached, validated policy snapshot; on startup, corruption,
  timeout, or model-service loss, use the static baseline without blocking writes.
- Provide a runtime kill switch that immediately stops learning and action use;
  no restart or consensus configuration change is required.

## Observability

Emit action records keyed by request correlation ID with configuration epoch,
feature/model versions, chosen proposer and delay, baseline action, exploration
flag, confidence, fallback reason, terminal outcome, latency, duplicate work, and
reward components. Do not log command payloads or unbounded feature vectors.

Metrics and dashboards must compare tuned and baseline cohorts for decision
latency (`p50`, `p95`, `p99`), success/timeouts, additional RPCs, hedged-request
rate, concurrent proposers, contention/round escalation, fallback rate, feature
age, action distribution, and reward drift. Alert on guardrail breaches, action
concentration, stale telemetry, and model/config mismatch.

## Rollout Stages

1. **Disabled:** execute static routing and do not explore.
2. **Shadow:** compute a model order but execute static routing. Shadow outcomes
   never train the unexecuted model action.
3. **ProposerCanary:** apply first-target selection to a deterministic attached-
   client cohort and learn only the first target's own observed outcome.
4. **ProposerDefault:** apply the proven target policy without exploration.
5. **HedgeCanary:** freeze the target policy before any future delay-only test.
   The current implementation still uses the fixed delay.
6. **BoundedDefault:** reserved for a separately accepted bounded hedge policy.

## Failure Handling

- Model timeout, crash, invalid output, stale features, or storage corruption:
  use the static action and increment a reason-coded fallback metric.
- Membership or configuration change: invalidate the current decision, reset the
  candidate set, and use static policy until cold-start gates pass.
- Guardrail breach or statistically credible regression: trip the kill switch,
  freeze model updates, preserve evidence, and revert all traffic to static policy.
- Conflicting concurrent attempts: let QuePaxa resolve them normally; never repair
  protocol state from bandit metadata.
- Telemetry or reward-pipeline failure: stop learning and exploration. Serving may
  use the last validated snapshot only while its freshness lease remains valid;
  otherwise fall back.

## Acceptance Criteria

Implementation may advance beyond shadow mode only when all of the following hold:

- Tests and fault injection demonstrate that every model failure and invalid
  action falls back without blocking, delaying beyond the configured bound, or
  changing QuePaxa protocol inputs and validation outcomes.
- Model outputs are structurally unable to change membership, quorum, ballots,
  certificates, recorder persistence, recovery, or retry/liveness rules.
- Shadow telemetry attributes at least 99.9% of eligible requests and reports
  feature freshness, action, outcome, and reward components without payload data.
- Canary results show a predeclared improvement in tail decision latency with no
  statistically credible regression in success rate or timeout rate and remain
  within explicit additional-RPC, duplicate-work, and contention budgets.
- Reconfiguration, restart, stale telemetry, model unavailability, and kill-switch
  drills all return to static policy within one action-selection interval.
- Rollback is automated, reason-coded, and exercised before each rollout stage;
  operators can compare tuned and static cohorts from existing dashboards.
