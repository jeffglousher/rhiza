# rkyv Recorder consensus-path direction

Date: 2026-07-25

Status: plaintext adapter implemented and selected as the default

Baseline: `v0.2.0` (`863a464a7d252bd2cd2db9b04ff6034899f1551d`)

## Decision

Promote plaintext `tcp-rkyv` to the absent-selector, configuration renderer,
and deployment default. `tcp-postcard` and `http` remain explicit,
mutually-exclusive rollback and diagnostic alternatives.

[rkyv](https://rkyv.org/motivation.html) supersedes Protobuf as the codec
candidate under evaluation. Stable single-cell measurements show a strong
diagnostic advantage for 4096-byte `record` and `fetch_command` at concurrency
4, while identity is neutral. The benchmark retains the provenance and tail
control limitations described below. On 2026-07-25, the user explicitly
accepted that bounded evidence and chose the default switch; this is an
operational decision, not a retroactive claim that the benchmark became
publishable or proved production superiority.

## Boundary

- Keep `rhiza-quepaxa` domain types and consensus identities codec-independent.
- Put the rkyv wire model and checked domain conversion inside the private
  `rhiza-node` Recorder transport adapter.
- Preserve the four-byte big-endian bounded frame and the existing
  authentication, fencing, request-id, operation-match, overload, and deadline
  contracts.
- Use safe validation for every network archive. The implemented rkyv frame is
  wire version 6 with a fixed 28-byte envelope and operation-specific archive
  body using schema version 2. The `RZRV` marker and exact-length checks remain
  part of the contract. Unchecked archive access is forbidden for Recorder
  input.
- Copy incoming bytes into `AlignedVec`, validate the archive, and complete
  bounded preflight checks before materializing owned domain values.
- Charge permit acquisition, validation, and conversion to the advertised
  deadline before materialization.
- A cluster or process runs exactly one Recorder mode: `tcp-rkyv`,
  `tcp-postcard`, or `http`. There is no codec negotiation, fallback on a
  connection, dual listener, or mixed-codec connection pool. A mismatch fails
  closed.
- The default bundle includes every member's `recorder_tcp_addr`, and the
  default workload listens on Recorder TCP port 8082.
- The rkyv bundle cannot be reused for HTTP rollback because strict HTTP
  validation rejects `recorder_tcp_addr`. HTTP rollback requires a separately
  prepared immutable, address-free HTTP bundle.
- A non-HTTP rendered manifest exposes only Recorder TCP port 8082 and removes
  the Recorder HTTP 8081 port and environment. It never configures both
  Recorder listeners.
- A mode change uses a coordinated stop/restart of the entire configuration or
  an atomic cutover to a new configuration. Sequential `OnDelete` replacement
  across incompatible modes is forbidden.
- Pin the rkyv version and format-affecting features, including endianness and
  pointer width. Read incoming archives into an alignment-preserving buffer.
- TLS is deliberately outside the promoted Recorder mode. Selecting rkyv with
  TLS or supplying TLS configuration is rejected.

## Mode cutover

An in-place Recorder mode change uses this fixed sequence:

1. Drain client traffic.
2. Scale the StatefulSet to `replicas=0`.
3. Confirm that every Pod has terminated and graceful shutdown completed.
4. Apply the new mode's immutable bundle Secret, Service, and StatefulSet
   manifest together. Do not change the Service before every old Pod has
   terminated.
5. Confirm that NetworkPolicy and host/firewall rules allow Recorder TCP 8082
   when the new mode requires it.
6. Scale the StatefulSet to `replicas=N`.
7. Require readiness and quorum health before restoring client traffic.

HTTP rollback uses the same procedure and a separate address-free immutable
bundle; the rkyv bundle fails strict HTTP shape validation. An atomic cutover to
a newly activated configuration remains the alternative. Local inspection
confirmed that the reference template uses `publishNotReadyAddresses: true`,
`podManagementPolicy: Parallel`, and `terminationGracePeriodSeconds: 30`. The
30-second Pod grace period exceeds the runtime shutdown budget of 25 seconds.

The current `RecorderRpc` trait consumes owned `String`, `Vec`, `StoredCommand`,
and proof values. Therefore the first adapter will still perform checked
archived-to-domain conversion. It must not be described as end-to-end zero-copy.
A borrowed domain boundary is a separate change and requires independent
evidence.

## Required behavior

The adapter must cover all eight current operations:

1. `Identity`
2. `StoreCommand`
3. `FetchCommand`
4. `Record`
5. `InstallDecisionProof`
6. `InspectDecisionProof`
7. `InspectRecordSummary`
8. `ObserveReadFence`

Tests must exercise observable behavior for every operation and reject:

- malformed, truncated, trailing, misaligned, and oversized frames;
- wrong codec or wire versions;
- invalid enum discriminants and out-of-range collection or string sizes;
- invalid domain conversion, proposer identity, membership, command, or proof;
- responses with mismatched request ids or operation kinds.

Permit acquisition, validation, and domain conversion are charged to the
advertised call deadline, starting from completion of the bounded frame read
and before owned value materialization.

## Performance evidence and remaining gates

Wire version 6 removes the giant-enum size floor found in the first adapter.
Identity requests and successful Store/Install acknowledgements now use the
28-byte envelope with no archive body; the corresponding wire version 5
messages were 290 and 698 bytes.

The controlled schema-3 runner uses exclusive raw candidates, concurrently
prewarms both connection leases, seeds AB/BA/AA/BB order, aggregates pair
first, and applies deterministic-bootstrap and per-pair/cell/field controls.
It fails closed when raw validity or same-candidate drift controls fail.

The first controlled local runs preserved useful functional diagnostics, but
did not establish a transport advantage:

- the full concurrency 1/4/32 run was invalid because rare 250 ms
  `ProposeFailed` errors occurred for both codecs in `record` at concurrency
  32;
- the concurrency 1/4 run was diagnostically and production-valid, but its
  comparison was invalid because AA/BB same-candidate drift exceeded the
  threshold;
- a focused 4096-byte fetch at concurrency 4 had a directional latency signal,
  but its controls also failed.

Wire inflation is fixed and the isolated codec CPU benefit remains real.
The subsequent stability protocol isolated one cell per process, used 10,000
warmup operations, measured at least 100,000 operations and five seconds, ran
11 seeded pairs, and applied 50,000 deterministic-bootstrap resamples with a
±10% drift gate. All three accepted summaries contained 88 raw results per
codec, zero errors, and an exact independent recomputation match.

Under that protocol:

- identity at concurrency 4 was neutral and passed its non-regression gate;
- 4096-byte `record` at concurrency 4 showed 1.076508× throughput and
  0.923335× p50 ratios, with primary controls passing;
- the accepted 4096-byte `fetch_command` retry at concurrency 4 showed
  1.091948× throughput and 0.917375× p50 ratios, with primary and p95 controls
  passing.

These are strong diagnostic results for the two 4096-byte cells. They remain
non-publishable benchmark evidence because all runs used the same seed and day
on a dirty development tree, and non-primary tail controls failed.

The user accepted those limits and selected the adapter as the default on
2026-07-25. A clean exact snapshot with a different seed and day remains
required before publishing a general performance or production-superiority
claim; it is no longer a blocker to the explicitly authorized default change.
The full result, controls, and reproduction guidance are in
[`recorder-transport-benchmark-2026-07-25.md`](recorder-transport-benchmark-2026-07-25.md).

The following evidence is still required before claiming production
superiority:

1. the real durable three-node QuePaxa workload;
2. a physical two-host run with identical persistence settings;
3. identical applied state, qlog, restart recovery, and checkpoints;
4. cross-build compatibility coverage for the pinned wire format.

These items remain follow-up validation work. They do not change the selected
plaintext `tcp-rkyv` default or make the benchmark limitations disappear.

## Rejected shortcuts

- Do not replace `postcard::from_bytes` with unchecked rkyv access.
- Do not add rkyv derives to QuePaxa domain types.
- Do not archive a Postcard byte blob inside rkyv; that preserves the decode
  cost while adding another format.
- Do not claim a speedup from rkyv's motivation alone. The official
  [validation](https://rkyv.org/validation.html) and
  [alignment](https://rkyv.org/format/alignment.html) constraints are part of
  the network contract.
