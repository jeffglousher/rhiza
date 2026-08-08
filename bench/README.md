# rhiza sql HTTP benchmark

`rhiza-bench` is a standalone Rust package for reproducible load tests of the rhiza sql HTTP API. It deliberately lives outside the main workspace and only uses the already-present `reqwest`, `serde`, and `serde_json` dependencies.

## Run

Provide at least one endpoint and a client token. The benchmark creates its table unless `--skip-setup` is set.

```sh
cargo run --release --manifest-path bench/Cargo.toml -- \
  --endpoint http://127.0.0.1:7101 \
  --token "$RHIZA_CLIENT_TOKEN" \
  --duration 60s \
  --warmup 10s \
  --concurrency 16 \
  --workload mixed \
  --write-percent 50 > benchmark.json
```

`RHIZA_BENCH_ENDPOINT` (a comma-separated endpoint list) or `RHIZA_ENDPOINT`, plus `RHIZA_CLIENT_TOKEN` or `RHIZA_BENCH_TOKEN`, can replace the endpoint and token flags. `--endpoint` may be repeated to define deterministic preferred-first failover order; every request starts at the first endpoint and tries later endpoints only for retryable failures.

Use either concurrency-driven load (omit `--target-rate`) or an aggregate open-loop start rate:

```sh
cargo run --release --manifest-path bench/Cargo.toml -- \
  --endpoint http://127.0.0.1:7101 --token "$RHIZA_CLIENT_TOKEN" \
  --duration 30s --warmup 5s --concurrency 8 --target-rate 200 \
  --workload write
```

Durations accept `ms`, `s`, `m`, and `h` suffixes; a bare value means seconds. The default duration is `30s`, warmup is `5s`, concurrency is `1`, request timeout is `10s`, and fault-command timeout is `5m` (`--fault-timeout`).

## Workloads

- `read`: sends `SELECT request_id, value FROM <table> WHERE request_id = ?` to `/v1/sql/query`.
- `write`: sends a unique command request id and `INSERT INTO <table> ... RETURNING request_id, value` to `/v1/sql/execute`. A write counts as a committed transaction only when the execute response has an applied index and returns its own request id.
- `mixed`: deterministically selects writes according to `--write-percent` (default `50`).

The default table is `rhiza_bench`; use `--table` for an isolated name. It must be a simple ASCII SQL identifier. Setup creates `request_id TEXT PRIMARY KEY, value TEXT NOT NULL` through the same HTTP API.

## Fault Hooks

Run a shell command at an offset from the beginning of the measured window:

```sh
cargo run --release --manifest-path bench/Cargo.toml -- \
  --endpoint http://127.0.0.1:7101 --token "$RHIZA_CLIENT_TOKEN" \
  --duration 60s --concurrency 12 --workload mixed \
  --fault 20s leader-restart "kubectl rollout restart statefulset/rhiza"
```

The JSON report records the supplied fault tag, configured `offset_seconds`, actual `command_start_offset_seconds`, completion flag, exit status, and `succeeded`, `failed`, or `unfinished` status. Requests are also aggregated into `before`, `during`, and `after` windows: `during` starts when the command is invoked and ends when it returns. Traffic stops at the measurement deadline; the command may finish afterward, bounded by `--fault-timeout`. An unfinished command and its descendants are killed and reaped before the harness returns.

## JSON Output

The program prints one JSON report to stdout and diagnostic/setup errors to stderr. Tokens are never included. `measurement.totals` and each fault window provide:

- attempts, successes, error count and bounded error classes;
- successful committed transactions and committed transactions per configured second;
- an exact-microsecond latency histogram with p50, p95, p99, and p99.9 estimates;
- configured and observed wall duration.

The command exits nonzero for invalid configuration, setup failures, failed fault commands, or fault commands that exceed `--fault-timeout`. The JSON report is printed before a fault failure is returned. Request failures, including expected failures during a fault scenario, are captured in the report so an interrupted-service run remains analyzable.

## Monthly Cost Calculator

`rhiza-cost` emits an itemized monthly USD estimate from retained GB-month,
object-call counts, and explicit egress input/rate. Its checked-in defaults are
[`rates-2026-07-12.json`](rates-2026-07-12.json), dated **2026-07-12**:

- AWS S3 Standard, `us-east-1`: `$0.023/GiB-month`, `$0.005/1k` PUT/LIST, `$0.0004/1k` GET.
- GCS Standard, `us-central1`: `$0.020/GiB-month`, with the GB-month input converted to GiB-month.
- Azure Blob Hot LRS, `eastus2`: `$0.0184/GiB-month`, with the same PUT/LIST/GET rates.
- DELETE is free in all defaults. RustFS local has zero call fees and requires its storage rate argument.

Egress defaults to zero and must be supplied as both a quantity and, when it is
not zero-cost, `--egress-usd-per-gb`.

```sh
cargo run --release --manifest-path bench/Cargo.toml --bin rhiza-cost -- \
  --provider aws-s3-standard-us-east-1 --retained-gb-month 100 \
  --put-count 2000 --list-count 1000 --get-count 1000 --delete-count 1000 \
  --egress-gb 10 --egress-usd-per-gb 0.09

cargo run --release --manifest-path bench/Cargo.toml --bin rhiza-cost -- \
  --provider rustfs-local --retained-gb-month 100 \
  --rustfs-storage-usd-per-gb-month 0.01
```

## Rhiza / Hiqlite comparison program

`scripts/bench-rhiza-hiqlite.sh plan` emits the versioned, machine-readable
comparison contract. It is safe by default: it does not build images, create a
cluster, or modify a deployment. The contract separates diagnostic, local
durable-quorum, volume-loss, and object-authoritative durability tiers; it also
marks Graph, persistent-KV versus cache, local versus consistent reads, and
object-authoritative recovery as non-comparable where their semantics differ.

```sh
scripts/bench-rhiza-hiqlite.sh plan target/rhiza-hiqlite-plan.json
```

The only mutating coordinator command is explicit. It delegates the fixed
`1,2,3 × 60,180,300` zero-PVC recovery matrix to the established Rhiza and
Hiqlite recovery runners; it does not contain duplicate deployment logic.

```sh
scripts/bench-rhiza-hiqlite.sh run-recovery
scripts/bench-rhiza-hiqlite.sh normalize-recovery \
  target/rhiza-e2e/sql/RUN/recovery-matrix.jsonl \
  target/hiqlite-recovery/RUN/summary.json target/recovery-normalized.json
```

`run-recovery` writes progress and the start event to stderr. On success its
stdout is one completed JSON manifest with the coordinator artifact root, both
exact source paths, and SHA-256 digests.

Normalization rejects missing, duplicate, or mismatched recovery coordinates.
It requires Hiqlite three-voter `emptyDir`/zero-PVC evidence and Rhiza zero-PVC
plus three-old/three-new-voter cell evidence, retains source paths, and writes
unavailable throughput/resource values as `not_measured`; it never invents them.
Recovery normalization is implemented; a matched workload/resource coordinator
is still pending, so no publishable performance comparison exists yet.

## vind Runner

`scripts/bench-vind.sh` creates a disposable vind cluster, deploys the existing
RustFS and three-node rhiza sql manifests through their existing render helpers,
then writes an `artifacts.json` manifest. It cleans up by default.

```sh
scripts/bench-vind.sh --duration 60s --warmup 10s --concurrency 4 \
  --fault pod-delete --fault-offset 20s
```

Set `RHIZA_RECORDER_TRANSPORT=tcp-postcard` to compare the cluster-internal
plaintext TCP/Postcard Recorder transport with the default HTTP transport.
The runner records the selected transport in `artifacts.json`.

The runner defaults to synchronous durability. Bounded and periodic runs must
provide their mode-specific positive duration; unrelated parameters are rejected:

```sh
RHIZA_DURABILITY_MODE=bounded RHIZA_DURABILITY_MAX_LAG=250ms \
  scripts/bench-vind.sh --duration 60s --workload write

RHIZA_DURABILITY_MODE=periodic RHIZA_DURABILITY_INTERVAL=2s \
  scripts/bench-vind.sh --duration 60s --workload write
```

Durability durations accept positive integer values with `ms`, `s`, `m`, or `h`
suffixes. `artifacts.json` records the selected mode and the applicable
`max_lag` or `interval` under `configuration.durability`; the rendered cluster
manifest records the same environment configuration.

The runner always establishes and monitors all three local node forwards for
readiness and final checkpoint consensus. The default workload still targets
only the preferred first endpoint; `RHIZA_BENCH_MULTI_ENDPOINT=1` passes all
three forwards to the load generator for preferred-first failover. A dead or
invalid admin endpoint makes the evidence fail instead of reducing the quorum
being checked. Final checkpoint roots must agree on both index and the serialized
32-byte hash across all three nodes.

`artifacts.json.provenance` binds the run to the Git commit and dirty state plus
the Docker image content ID and available repository digests. A dirty source
tree or missing immutable image ID leaves an ordinary local run usable but sets
`publishable: false` with a reason. Publication also requires a successful
benchmark and runner exit, no failed evidence collection, and verified cleanup;
`--keep` runs are therefore not publishable. Only artifacts marked
`publishable: true` support release or published performance evidence. A
skip-build run additionally requires the image's
`org.opencontainers.image.revision` label to match the exact Git commit.
The same provenance records the benchmark client SHA-256, Rust toolchain
versions, and normalized Kubernetes runtime image digests. rhiza sql and RustFS
are always required; disabled object metering marks its nginx and AWS inventory
images `not_applicable` instead of requiring them. All three rhiza sql pods must
run the same digest as the locally inspected image.

It applies fixed default resources to make comparisons controlled on the
8-core/24-GiB host: each rhiza sql and RustFS container requests `250m` CPU and
`512Mi` memory, with `1000m` and `1Gi` limits. RustFS is only the local S3
simulator and its resources are reported separately from rhiza sql. Override them through
`RHIZA_BENCH_{RHIZA,RUSTFS}_CPU_{REQUEST,LIMIT}` and
`RHIZA_BENCH_{RHIZA,RUSTFS}_MEMORY_{REQUEST,LIMIT}`. Resource JSONL
samples use containerd CRI stats and their runtime-provided metric timestamp,
rather than the time the potentially slow collection started. CPU and memory
values must share that timestamp. Every stats response also receives a unique
collection batch ID, so app memory is summed across containers from the same
response even when their CRI timestamps are staggered. Missing, invalid, or
reused batch IDs invalidate the resource evidence. Each batch requires all three
rhiza sql ordinals plus RustFS and, when enabled, its object-meter sidecar. Only
the named pod-delete target may be absent, only while the batch timestamps are
inside the verified fault window; that incomplete rhiza sql batch is excluded
from memory averages and peaks instead of being counted as a partial snapshot.
`resource-summary.json`
reports container-lifecycle CPU deltas plus average/peak memory using samples
inside, or immediately bracketing, the Rust-reported measurement window. A
complete app collection's earliest and latest CRI timestamps classify that
memory batch; missing complete predecessor or successor batches invalidate the
summary. CPU windowing continues to use each container's runtime timestamp. A
pre-existing container uses its last pre-window counter as the baseline; a
container born in the window uses zero, and a same-identity counter regression
invalidates the evidence. Pod UID, container ID, and restart count must remain
stable for every component. A pod-delete run requires exactly one identity
transition for the named fault pod inside the verified fault window; any other
restart or identity transition invalidates the evidence. Warmup and later
cleanup samples are excluded.
Publishable evidence permits one missed collection plus one second of scheduling
jitter. The three-second collection timeout sends TERM and forces a kill one
second later, so the default continuity and final-coverage budget is twice the
two-second interval plus twice the four-second hard collection bound plus one
second (13 seconds). Final coverage may additionally finish the last hard-bounded
collection, for a 17-second wait budget. A pod-delete gap is accepted only for
the named rhiza sql ordinal and only when it brackets the measured fault window
within the continuity budget; other missing component samples invalidate
publication. Disable resource sampling with
`RHIZA_BENCH_RESOURCE_SAMPLING=0`. A default-on nginx sidecar meters S3 method,
status, and byte counts, while an AWS CLI inventory records logical object count
and retained bytes in `object-usage.json`. Disable it with
`RHIZA_BENCH_OBJECT_USAGE_METERING=0`. The runner asserts that the deployment
uses zero PVCs. Its only fault hook deletes a rhiza sql pod and waits for the
replacement to become Ready; it does not inject RustFS failures.

## Test

```sh
cargo test --manifest-path bench/Cargo.toml
```

## Direct SQL, graph, and KV profile benchmark

`rhiza-profile` compares the public embedded API (`--layer handle`), direct
`NodeRuntime` calls (`--layer runtime`), and the materializer (`--layer raw`)
without HTTP. Every run preloads the same 256 bounded keys and measures a fixed
number of operations. Handle and runtime writes use a local QuePaxa instance
backed by three file-based Recorder voters; raw excludes consensus. Reads default
to local consistency; pass `--consistency read_barrier` on handle/runtime read
workloads to measure the read-only 2/3 quorum fence. The stable JSON report
records the selected consistency, these boundaries, the exact Git state,
host/toolchain provenance, errors, throughput, and p50/p95/p99/p99.9/max latency
in microseconds.

Build once, then run each profile under the same operation count, payload, and
concurrency. `native-read` is a supplemental bounded ordered query for SQL and
graph, and a bounded prefix scan for KV; compare `get` for point-read parity.
For graph, `get` measures generic Cypher while `document-get` measures the fixed
document projection without parsing a caller-supplied query.

For `write`, `--batch-size 1|2|4|8|16|32|64` exercises every typed batch API on
the handle or runtime. SQL and KV additionally accept 128 and 256: a
public typed SQL call is capped at 256 members and 512 KiB of aggregate
canonical encoded input. Its FIFO group queue has a fixed 32 MiB pending
encoded-byte budget, while one physical group is capped at 2 MiB and 1,024
members. Direct SQL QWAL additionally accepts 512 and 1,024 members.
`operations`, attempts, successes, errors, and
`operations_per_second` are logical-command counts. `batch_calls`,
`successful_batch_calls`, `failed_batch_calls`,
`batch_calls_per_second`, and `batch_call_latency_us` describe physical API
calls separately. `logical_item_latency_us` is the batch service time amortized
over its submitted members; it is not an independent end-to-end latency sample
for every member.

KV public calls are capped at 256 members. Direct runtime and embedded
`mutate_kv`/`mutate_kv_batch` calls enter a 64-call, 32 MiB FIFO queue with a
500µs quiet period that restarts on arrival and a 2ms hard deadline at the
default setting; one active group contains at most 1,024 members. The
wire batch is command version 3 and the redb materializer fingerprint uses
domain v3, so this is a clean-install breaking boundary. One `Immediate` redb
transaction durably stores the full qlog entry with the KV state, receipts, and
applied tip. The file qlog is a buffered serving mirror and can be rehydrated
from redb, removing its separate hot-path sync without weakening strict ACK.
SQL uses a Recorder-authoritative model: the 2/3 Recorder WAL sync is the only
durability boundary on the common path. SQLite, the generation-6 control
sidecar, and the file qlog are non-durable local views rebuilt from a verified
checkpoint plus Recorder tail. ACK waits for local SQLite visibility, while
readiness remains closed until tip validation and catch-up finish. Recorder
QCMD files currently are retained without a GC path.
The 512 KiB replicated command ceiling still applies: an oversized flattened
group is emitted as the largest fitting ordered prefixes. HTTP writes use their
existing async writer batch directly and do not enter the KV queue a second
time. Graph is not part of this KV group-commit path.

For runtime writes, `qlog_entries` and `logical_operations_per_qlog` expose
coalescing efficiency. SQL QWAL reports `qwal_prepare_latency_us`,
`qwal_apply_latency_us`, and actual encoded `qwal_envelope_bytes`, in addition
to whole-call latency. A clean-install QWAL v3 SQL batch is ordered and
non-atomic: successful members share one qlog entry and one anchor, while member
savepoints isolate failures. An all-failed batch creates no qlog entry. Retry an
indeterminate batch as the whole unchanged vector with the same request IDs.
Profile report schema v6 records `configuration.sql_padding_mib`, the optional
`configuration.follower_apply_latency_scope`, and
`configuration.follower_seed_state`, plus
`measurement.follower_apply_latency_us`. `follower_seed_state` records the
pre-warmup leader/follower DB and control sizes, the number of expected seed
receipts verified by request ID, command digest, and original log-index ordinal,
and the zero embedded-qlog counts after restore. When an explicit seed cache is
used, its `seed_cache` object also records `created` or `hit`, the absolute file
path, whole-cache digest and byte count, and the embedded snapshot digest. The
report continues to name
the buffered SQL mirror cost `phase_latency_us.local_qlog_mirror_append`; that
phase is not a durability sync.

`--layer follower-apply` seeds one temporary SQL leader with leader-generated
QWAL entries. It then creates one recovery snapshot and restores it into fresh
node-1/node-2 views; no temporary seed follower is created. This clears the seed
embedded qlog; expected request receipts intentionally remain, and the verified
expected population plus control-file cost are reported. Each measured QWAL is
prepared on the leader. Payload
clone, hash and `LogEntry` construction finish before the timer; only the
follower `SqliteStateMachine::apply_entry` call is timed. Anchor bookkeeping and
the subsequent leader catch-up also remain outside `follower_apply_latency_us`.
Whole-run throughput includes the complete loop, so the two metrics must not be
interchanged. `--sql-padding-mib 0..1024` adds exactly that many MiB of zero
blobs to an untouched deterministic `bench_padding` table before snapshot
restore and warmup. Measured writes keep the same `--value-bytes` hot-row payload
in `bench_items`; seed and restore are excluded from reported latency samples.

Large follower-apply comparisons can reuse their deterministic setup with
`--sql-seed-cache FILE`. If the file is absent, the harness performs the normal
QWAL seed and creates the normalized recovery snapshot. It first restores and
fully validates fresh node-1/node-2 materializers, then publishes the validated
cache atomically without replacing an existing file. If the file exists, the
harness validates a configuration-derived hard size bound before allocating the
snapshot, reads the header and exact snapshot once through one revalidated file
handle, then skips QWAL seeding and restores fresh materializers from it. The
cache binds the exact seed-recipe ID and digest, active configuration state,
padding, value size, keyspace, expected receipt count, snapshot manifest, and
canonical framing/digests. Both paths fail closed unless the restored tips
align, every expected receipt remains at its exact original ordinal, the
embedded qlog is empty, and every deterministic seed and padding row has the
requested ID, length, and content. This proves the expected recipe; it does not
claim a separately enumerated total receipt count because that count is not
exposed by the materializer API. A malformed, modified, sparse oversized,
symlinked, non-regular, or configuration-mismatched cache is never regenerated
over the existing path. Reported paths are canonical.

Use different cache files for baseline and current binaries. Sharing one cache
would bind both runs to one harness/report format and could hide a setup-format
difference. For example:

```sh
baseline/bench/target/release/rhiza-profile \
  --profile sql --workload write --layer follower-apply --batch-size 1 \
  --operations 10000 --warmup 1000 --concurrency 1 --value-bytes 128 \
  --sql-padding-mib 1024 \
  --sql-seed-cache target/rhiza-bench/profile/baseline-padding-1024mib.seed \
  > target/rhiza-bench/profile/baseline-follower-apply-padding-1024mib.json

bench/target/release/rhiza-profile \
  --profile sql --workload write --layer follower-apply --batch-size 1 \
  --operations 10000 --warmup 1000 --concurrency 1 --value-bytes 128 \
  --sql-padding-mib 1024 \
  --sql-seed-cache target/rhiza-bench/profile/current-padding-1024mib.seed \
  > target/rhiza-bench/profile/current-follower-apply-padding-1024mib.json
```

The current release evidence is under
`target/rhiza-bench/write-v3-group-window-idle/20260719T032700/` (an ignored
benchmark directory). Runtime c4 submitted four concurrent public 256-member
calls; the bounded 500µs drain window produced a median **15,824 logical
ops/s**, 101 QLog entries per 102,400 writes, and a seven-run IQR below 1%. Direct QWAL
medians from `target/rhiza-bench/write-v3-group-commit/20260719T025727/` were
16,313 / 22,109 / 25,730 logical ops/s for 256 / 512 / 1,024 members. Read each
artifact's `README.md` for exact commands, durability boundaries, topology, raw
JSON, and limitations.

The first post-`HashSet` c4 diagnostic under
`target/rhiza-bench/write-v4-hashset/20260719T042000/` had a seven-run median of
**17,974 logical ops/s**, **13.6%** above 15,824. It does not replace the stable
official result: an orphan Virtualization VM was present and the post-run
snapshot showed heavy `syspolicyd` and `trustd` activity. Treat it only as a
signal for a controlled rerun.

The checked-in [strong-read diagnostic](strong-read-results-2026-07-19.md)
records three-run c1/c4/c16 local and read-barrier controls for SQL, KV, and
Graph after disk space was cleared. The host was not idle and later showed an
orphan VM/background load, so the result is diagnostic rather than publishable
release evidence.

The post-change KV artifact is
`target/rhiza-bench/kv-group-commit/20260719T122120/`, built as binary SHA-256
`a1f34866955b638371db4e0852f04d382425d22a0c5247aced5b828009c4db76`.
For 102,400 logical writes in 1,600 public batch calls, c1 median was **2,008.81
ops/s** with 1,600 qlog entries and c4 median was **10,738.69 ops/s** with
401–402 qlog entries. This is a structural/qlog gate pass, not publishable
throughput evidence: c1/c4 IQR was 11.09%/7.24%, and Dory VM,
`syspolicyd`, and Storage extension activity invalidated the stability gate.
The checked-in [SQL/KV diagnostic](sql-kv-results-2026-07-19.md) preserves the
full comparison and raw-artifact boundary. Graph is excluded from that report.

```sh
cargo build --release --locked --manifest-path bench/Cargo.toml \
  --bin rhiza-profile

mkdir -p target/rhiza-bench/profile
for profile in sql graph kv; do
  bench/target/release/rhiza-profile \
    --profile "$profile" --workload write \
    --operations 10000 --warmup 1000 --concurrency 8 --value-bytes 128 \
    > "target/rhiza-bench/profile/${profile}-write.json"
  bench/target/release/rhiza-profile \
    --profile "$profile" --workload get \
    --operations 10000 --warmup 1000 --concurrency 8 --value-bytes 128 \
    > "target/rhiza-bench/profile/${profile}-get.json"
done

for layer in handle runtime raw; do
  bench/target/release/rhiza-profile \
    --profile graph --workload document-get --layer "$layer" \
    --operations 10000 --warmup 1000 --concurrency 1 --value-bytes 128 \
    > "target/rhiza-bench/profile/graph-document-get-${layer}.json"
done

for batch_size in 1 2 4 8 16 32 64; do
  bench/target/release/rhiza-profile \
    --profile kv --workload write --layer runtime --batch-size "$batch_size" \
    --operations 10000 --warmup 1000 --concurrency 1 --value-bytes 128 \
    > "target/rhiza-bench/profile/kv-write-batch-${batch_size}.json"
done

bench/target/release/rhiza-profile \
  --profile sql --workload write --layer follower-apply --batch-size 1 \
  --operations 10000 --warmup 1000 --concurrency 1 --value-bytes 128 \
  --sql-padding-mib 1024 \
  > target/rhiza-bench/profile/sql-follower-apply-padding-1024mib.json
```

Run on an otherwise idle machine. Each invocation uses a fresh temporary data
directory, and reports from a dirty worktree remain useful locally but should
not be published as release evidence. The example writes reports under the
repository's ignored `target/` directory so shell redirection itself does not
make a clean checkout appear dirty. This direct benchmark excludes HTTP,
serialization, node-to-node network latency, remote checkpoints, and
multi-host behavior; use the vind runner for those costs.

## Node transport microbenchmark

`rhiza-transport` compares the private node-RPC building blocks on loopback. It
runs plaintext HTTP and HTTPS with JSON, Postcard, and Prost bodies, plaintext
and rustls-protected persistent TCP with Postcard and Prost, Quinn with one
stream per RPC, Quinn with one persistent lane per worker, and a plaintext
`postcard-rpc` framework candidate. HTTPS, TLS/TCP, and Quinn trust the same
generated certificate, so their steady-state TLS costs are comparable. TCP
candidates use the same four-byte big-endian length prefix, one warmed
connection or session per worker, `TCP_NODELAY`, frame limit, validation, and
timeout paths.

```sh
cargo run --release --locked --manifest-path bench/Cargo.toml \
  --bin rhiza-transport -- \
  --warmup 4096 --operations 60000 \
  --payloads 128,4096 --concurrency 1,8,64 \
  > transport-run.json
```

Rotate the effective candidate order across three external runs to reduce
first/last-run bias while preserving each run as an independent report:

```sh
for offset in 0 1 2; do
  cargo run --release --locked --manifest-path bench/Cargo.toml \
    --bin rhiza-transport -- \
    --warmup 4096 --operations 60000 \
    --payloads 128,4096 --concurrency 1,8,64 \
    --candidate-order-offset "$offset" \
    > "transport-run-$offset.json"
done
```

For the controlled TLS comparison, use the standard-library runner. It runs
exactly the six TLS candidates at offsets `0`, `2`, and `4`, preserves all raw
reports, validates row completeness, warmup and measurement errors, and TLS
handshake deltas, then writes medians and worst maximum latency without pooling
samples:

```sh
cargo build --release --locked --manifest-path bench/Cargo.toml \
  --bin rhiza-transport
python3 bench/run-rpc-tls.py --output-dir bench/rpc-tls-results
```

Use `python3 bench/run-rpc-tls.py --self-test` for the embedded aggregation
fixture. The summary includes binary SHA-256, Git and host provenance, and the
effective order of every run.

For an isolated Postcard-versus-Prost comparison over otherwise identical
plaintext and TLS/TCP stacks, run:

```sh
cargo build --release --locked --manifest-path bench/Cargo.toml \
  --bin rhiza-transport
python3 bench/run-rpc-codec.py --output-dir bench/rpc-codec-results
```

`run-rpc-codec.py` runs `tcp-postcard`, `tcp-prost`, `tcp-tls-postcard`, and
`tcp-tls-prost` four times, rotating every candidate through every order
position. Its defaults cover 128- and 4096-byte payloads at concurrency 1, 8,
and 64. It preserves raw JSON,
validates framing metadata, response/error counts, topology, codec identity,
and TLS handshake/ALPN telemetry, then emits four-run per-cell medians and worst
maximum latency. Use `python3 bench/run-rpc-codec.py --self-test` to test its
aggregation fixture without running the Rust benchmark.

The schema-version-2 codec summary keeps the flat absolute medians and declares
exactly two comparisons: plaintext `tcp-postcard` to `tcp-prost`, and TLS
`tcp-tls-postcard` to `tcp-tls-prost`. It first pairs Prost and Postcard within
each run and payload/concurrency cell, then reports Prost/Postcard throughput
and p50/p95/p99/p99.9 ratios with per-run values, median, minimum, maximum, and
median percent delta. An equal-cell-weight geometric mean is included only as
an auxiliary summary, so a reversal in a cell such as 4096 bytes at concurrency
64 remains visible rather than being pooled away.

`comparison_valid` means diagnostics and clean, consistent provenance passed
and both declared within-security comparison groups are valid. It never makes
the four candidates one comparison and does not validate a plaintext-versus-TLS
claim; `cross_security_comparison_valid` is always false.

For the local framework A/B, compare the hand-written `tcp-postcard` framing to
`tcp-postcard-rpc` while keeping the request and acknowledgement schema, payload,
four-byte big-endian length prefix, frame limit, timeout, concurrency, plaintext
security stratum, and one warmed session per worker fixed:

```sh
cargo build --release --locked --manifest-path bench/Cargo.toml \
  --bin rhiza-transport
python3 bench/run-rpc-framework.py \
  --output-dir bench/rpc-framework-results
```

The framework candidate is pinned to `postcard-rpc` 0.12.1 with `use-std`. It
uses the real `HostClient`, custom TCP `WireRx`/`WireTx`, `Server`, and generated
endpoint dispatcher. Requests alternate between `rhiza/record` and
`rhiza/record/replicate`; both paths use the identical `WireRequest`/`WireAck`
schema so dispatch is exercised without changing payload work. At concurrency
greater than one, two cloned client lanes can have requests outstanding on the
same worker session, while a shared semaphore keeps total in-flight work at the
configured concurrency.

After warmup negotiates the dispatcher's one-byte key, each measured
postcard-rpc frame has a six-byte header (one-byte discriminant, one-byte key,
four-byte sequence) plus the common four-byte length prefix. Raw JSON reports
both components separately and includes them in encoded request/response sizes.

`run-rpc-framework.py` defaults to three alternating A/B pairs (six runs), so
each candidate occupies each order position three times. It preserves raw JSON,
validates schema, framing overhead, endpoint paths, topology, errors, and
provenance, then reports paired `tcp-postcard-rpc / tcp-postcard` ratios per
payload/concurrency cell plus an equal-cell-weight geometric mean. Use
`python3 bench/run-rpc-framework.py --self-test` for its aggregation fixture.
A dirty or inconsistent Git tree remains diagnostically useful locally but sets
both `comparison_valid` and `publishable` to false.

The preceding `rhiza-transport` Postcard comparison is **framework-only**. Do
not aggregate it with production Recorder adapter measurements. For the actual
adapter A/B, `rhiza-recorder-transport` starts the public production legacy and
`postcard-rpc` servers and calls them through their public production clients.
That preserves the real HELLO exchange, opaque Postcard envelope, manual
seven-endpoint dispatcher, sync bridges, deadlines, connection pools, and
candidate overload behavior. It measures `record` on the consensus lane and
`inspect_record_summary` on the control lane against identical deterministic
in-memory `RecorderRpc` fixtures.

```sh
cargo build --release --locked --manifest-path bench/Cargo.toml \
  --bin rhiza-recorder-transport
python3 bench/run-recorder-transport.py \
  --output-dir target/rhiza-bench/recorder-transport-results
```

The runner defaults to three balanced plaintext A/B pairs at concurrency 1, 4,
and 32. Add `--security plaintext,tls` to run the TLS 1.3 pair as a separate
stratum; plaintext and TLS results are never combined. Every candidate has one
shared production client object reused by all threads and cells. Both lanes are
warmed before every metric. The candidate's real bridge depth and in-flight
limit are intentionally not widened: `try_send` overloads remain classified
errors, attempt throughput includes them, success throughput excludes them, and
latency percentiles contain successful calls only. The report records the
production Key8/Seq4 13-byte `postcard-rpc` header and the separate four-byte
frame length prefix. Use `python3 bench/run-recorder-transport.py --self-test`
to test validation, balancing, and aggregation without running Rust.

Raw runs from a dirty tree remain useful for local diagnosis, but the runner
sets `comparison_valid` and `publishable` false. It also requires the Git commit
and dirty state to remain identical across all runs and binds the summary to the
release binary's SHA-256.

The codec runner writes each completed process's stdout before validation. If the
benchmark process fails or emits invalid JSON, it exits nonzero immediately;
there may therefore be fewer than four raw files and no `summary.json`. This
failure-artifact behavior is intentional and callers must preserve the output
directory and check the runner exit status.

This is a single-process loopback decomposition benchmark. HTTP and TCP are
kept as plaintext decomposition controls. TLS certificate creation and
handshakes finish before the measured window; HTTPS pools, TLS/TCP worker
connections, and Quinn connections/lanes are warmed and reused. The benchmark
does not measure mTLS, QuePaxa quorum work, persistence, fsync, or database
materialization. The JSON report repeats these limitations, records the order
offset plus effective order, and validates every response including its request
ID. Each call has a two-second timeout. Raw reports distinguish
`diagnostic_valid` from `comparison_valid`: warmup or measurement errors fail
the diagnostic, while a dirty tree, fewer than four codec runs, or unverified
TLS negotiation prevents the declared codec-pair comparisons from being valid.
HTTPS and TLS/TCP
observe negotiated TLS version and ALPN from rustls; Quinn observes ALPN and
relies on QUIC's TLS 1.3 invariant while using an explicitly TLS 1.3-only
configuration.
