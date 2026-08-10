# Rhiza / Hiqlite benchmark standard

This is the normative, improvement-oriented comparison standard. It measures
the product contract before speed. It does not produce one cross-semantic
winner.

The executable representation is emitted by
`scripts/bench-rhiza-hiqlite.sh plan`. Every published result must retain that
plan, all raw artifacts, exact source/image provenance, host hardware, kernel,
filesystem, client path, topology, durability contract, workload seed, and
start/finish timestamps. Run every cell at least three times in rotated order;
publish median and IQR as well as all failed attempts.

The initial Hiqlite reference is release `0.14.0`, commit
`c8316c53799c509990475ea8e2aa2ef8679e070e`, built from that exact source.
For every trial, generate and retain `Cargo.lock` during the first exact-source
build, record its digest and resolved OpenRaft version, and label that value
`openraft_version_source: generated-cargo-lock`. Exact local-reuse cells receive
the same verified lock path and digest and re-extract the version from that
preserved file; the coordinator requires every phase to agree with the recorded
version and digest. Run its
`immediate`, `immediate_async`, and configured interval WAL modes as separate
durability leagues; never compare an interval result with Rhiza's durable-quorum
ACK path. Rhiza provenance is the exact tested commit and dirty-state flag, not
a moving branch name.

## Contract and comparison leagues

| Tier | ACK survival boundary | Direct comparison |
| --- | --- | --- |
| D0 | In-memory diagnostic | Yes, diagnostic only |
| D1 | Local durable quorum | Yes, with identical sync boundary |
| D2 | One-volume-loss rejoin | Yes, with identical failure injection |
| D3 | Full-volume restore | No: Rhiza object-authoritative checkpoint and Hiqlite backup differ |
| D4 | RPO0 object-authoritative | No: Hiqlite has no equivalent per-write object ACK contract |

Local reads and strong reads are separate leagues. Direct/embedded measurements
and HA HTTP/TLS measurements are separate leagues. Rhiza Graph has no Hiqlite
counterpart; benchmark it against standalone LadybugDB, then state-machine,
runtime, and HA layers. Rhiza persistent redb KV may only be compared with a
Hiqlite disk-backed cache under matched TTL, restart, and recovery semantics;
Hiqlite memory cache is diagnostic only.

## Mandatory matrices

For SQL, KV, and Graph where supported, measure engine, state-machine, direct,
and HA HTTP/TLS paths. Use single write, transactions, batches, local reads,
strong reads, 90/10, 50/50, and 10/90 mixed load; scans and traversal for Graph.
Sweep logical batch sizes 1/2/8/32/64/256, concurrency 1/4/16/64/256, payloads
64B/1KiB/16KiB/256KiB, 3/5/7 voters, 0.1/1/5/20/50ms RTT, and loss/jitter/reorder
or partition. Repeat at 1/10/100GiB and 30-minute/6-hour/24-hour soak.

Inject preferred-proposer or leader loss, follower loss, one/two/three peer
loss, a volume loss, object-store outage, checkpoint during failure,
snapshot/log corruption, and rolling replacement. The executable recovery gate
is exactly failures 1, 2, 3 crossed with holds 60, 180, 300 seconds.

## Required scorecards and metrics

Keep five independent scorecards: correctness/durability, steady-state/tail,
protocol/apply, failure/recovery, resource/object cost. Every result includes
logical and physical log throughput, latency p50/p95/p99/p99.9/max, successes,
errors, timeouts, retries, ACK-to-visibility, queue depth, apply lag, sync
count/time, CPU/op, RSS, disk/network bytes/op, object calls/bytes/retained
bytes, RPO, service RTO, full RTO, and full-redundancy RTO.

Correctness is a gate: validate acknowledged-write ledger, idempotent retry,
strong-read correctness, and final state/log hash before using a performance
number. Never drop failed requests from goodput.

SQL writes must be deterministic on both systems. Externalize timestamps and
random values before submission, use identical schema/indexes and prepared
statement policy, and report cold/warm page-cache and statement-cache states
separately. Split relational SQL, persistent KV/cache, locks/counters, and
notifications into independent tests. Record leader/follower or local/strong
read routing and every client retry or lost response.

## Adoption hard gates and executable coverage

No implementation improvement is adopted from a headline throughput number.
The correctness ledger and final state validation must pass; durability,
topology, client path, and workload must be matched; and three rotated runs with
raw provenance must agree. A recovery claim additionally requires the complete
recovery matrix to pass.

Recovery normalization and the D1 SQL write/tail runner are implemented now.
There is still **no published Rhiza-versus-Hiqlite result**, and comparable
resource telemetry remains pending. The recovery normalizer emits
`not_measured` instead of inventing workload, CPU, or object-store metrics.

## Execution and normalization

`scripts/bench-vind.sh`, `scripts/e2e-vind-rustfs.sh`, and
`scripts/e2e-hiqlite-recovery.sh` remain the only deployment owners. The
coordinator is deliberately safe by default:

```sh
scripts/bench-rhiza-hiqlite.sh plan target/rhiza-hiqlite-plan.json
scripts/bench-rhiza-hiqlite.sh run-recovery
```

This command executes one diagnostic trial. Its normalized output is always
marked non-publishable; a claim requires three order-rotated trials with the
same contract and complete raw provenance. Each of its nine recovery
coordinates is a separate, freshly created vcluster for both systems; a
stateful nine-cell sequence is rejected.

The source-freeze fingerprint covers the full tracked delta from `HEAD`
(staged and unstaged) plus untracked source files; only `target/` artifacts are
excluded. Hiqlite raw `recovery.jsonl` is accepted only when its `run_started`
identity and exact `phase_summary` match that cell's `summary.json`. A path and
SHA-256 without this semantic linkage is insufficient evidence.

Every normalized recovery cell must preserve the source runner artifact path
and SHA-256 and carry the same isolation proof:

```json
{"mode":"fresh-vcluster","process_generation_new":true,"storage_generation_new":true,"restore_env_absent":true,"prior_sentinel_absent":true,"exact_membership":true,"object_provenance_current":true,"cleanup_verified":true}
```

The coordinator builds one coordinator-tagged Rhiza image for its first cell
and reuses it only after its local Docker ID matches exactly. It builds the
pinned Hiqlite voter/proxy pair and generated lockfile in its first cell, then
requires exact voter/proxy image IDs, pinned commit, and lockfile digest for
every later reuse. Missing proof invalidates the diagnostic trial rather than
being inferred.

The coordinator freezes a canonical source fingerprint before the first cell
and verifies it before and after every adapter: Git HEAD, the combined staged
and unstaged binary diff from HEAD, digest-only sorted untracked paths outside
`target`, and recursive submodule status are all part of that fingerprint.
It records the accepted fingerprint in the completion artifact.

After an explicit recovery run, normalize supplied source artifacts with:

```sh
scripts/bench-rhiza-hiqlite.sh normalize-recovery RHIZA_JSONL HIQLITE_SUMMARY OUTPUT
```

It validates all nine exact cells, Hiqlite three voters/`emptyDir`/zero PVC and
Rhiza zero-PVC plus three-old/three-new-voter cell evidence where exposed. It
preserves input paths and emits
`not_measured` for throughput/resource values absent from recovery artifacts.
No inferred metrics may be used in a scorecard.
