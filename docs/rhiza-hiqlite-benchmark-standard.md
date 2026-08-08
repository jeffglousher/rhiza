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
`c8316c53799c509990475ea8e2aa2ef8679e070e`, OpenRaft `0.9.24`, built from
that exact source. Record the generated `Cargo.lock` digest. Run its
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

Recovery normalization is implemented now. The comparable workload/resource
runner is pending, so there is **no publishable Rhiza-versus-Hiqlite performance
comparison yet**. The recovery normalizer emits `not_measured` instead of
inventing workload, CPU, or object-store metrics.

## Execution and normalization

`scripts/bench-vind.sh`, `scripts/e2e-vind-rustfs.sh`, and
`scripts/e2e-hiqlite-recovery.sh` remain the only deployment owners. The
coordinator is deliberately safe by default:

```sh
scripts/bench-rhiza-hiqlite.sh plan target/rhiza-hiqlite-plan.json
scripts/bench-rhiza-hiqlite.sh run-recovery
```

After an explicit recovery run, normalize supplied source artifacts with:

```sh
scripts/bench-rhiza-hiqlite.sh normalize-recovery RHIZA_JSONL HIQLITE_SUMMARY OUTPUT
```

It validates all nine exact cells, Hiqlite three voters/`emptyDir`/zero PVC and
Rhiza zero-PVC plus three-old/three-new-voter cell evidence where exposed. It
preserves input paths and emits
`not_measured` for throughput/resource values absent from recovery artifacts.
No inferred metrics may be used in a scorecard.
