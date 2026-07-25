# Recorder codec microbenchmark

Date: 2026-07-25

Status: stage-1 diagnostic complete; default switch accepted separately

## Decision

rkyv wins the stage-1 codec CPU gate for the corrected, production-shaped
`Record` request cells. Later stable single-cell transport measurements found
a strong diagnostic advantage for 4096-byte `record` and `fetch_command` at
concurrency 4, while identity was neutral. On 2026-07-25, the user accepted the
documented same-seed/day and dirty-tree limitations and selected plaintext
`tcp-rkyv` as the default. This document remains codec-only diagnostic evidence
and must not be read as proof of production superiority.

The stage-1 result was recorded in Yeoul as `fact_004122`, supported by
correction episode `ep_004121`: rkyv passed the isolated eight-operation
Recorder codec CPU gate. That historical fact did not itself imply a transport
promotion or default change; the later default decision is recorded separately
in [`rkyv-recorder-path-2026-07-25.md`](rkyv-recorder-path-2026-07-25.md).

Independent review found semantic-fidelity and optimizer-resistance problems in
the first harness. The implementation was corrected and the report schema was
advanced to version 2. All measurements from the earlier shared-exchange DTO
were discarded; none are used in this document.

## Benchmark structure

`bench/src/bin/rhiza-recorder-codec.rs` is a standalone, in-process codec
microbenchmark over separate, private, owned request and response DTOs. They
mirror the production Recorder TCP `RequestFrame`, `RecorderRequestBody`,
`ResponseFrame`, `RecorderResponseBody`, `RpcResult`, and nested public
QuePaxa fields. No benchmark wrapper bytes are serialized. The fixtures remain
benchmark-private and are not a production wire contract.

The harness covers all eight current Recorder operations:

1. `identity`
2. `store_command`
3. `fetch_command`
4. `record`
5. `install_decision_proof`
6. `inspect_decision_proof`
7. `inspect_record_summary`
8. `observe_read_fence`

Each operation has a request and successful response cell. Only these three
production-shaped directions carry 0-byte, 128-byte, and 4096-byte
`StoredCommand` payload cells:

- `store_command` request;
- successful `fetch_command` response;
- `record` request.

Every other semantic request or response is measured exactly once without a
payload label. Replacing those three single cells with three payload variants
produces 22 semantic cells and 44 codec metrics across Postcard and rkyv.
Each metric separately measures encode, full owned decode, and fresh
encode-plus-decode total time.

Every timed encode consumes the complete encoded allocation through
`black_box`. Every timed decode consumes the complete fully owned decoded value,
and the total phase consumes both complete values. Before timing, each
codec/cell pair must pass full owned round-trip equality. The harness also
records an encoded FNV-1a digest outside the timed phases.

For schema v2, `diagnostic_valid=true` requires all of the following:

- Cargo profile `release` with optimization enabled;
- semantic cell identities exactly matching the 22-cell production-shaped
  plan;
- exactly one metric for every codec/cell pair, with no missing or duplicate
  entries, for 44 metrics total;
- successful full owned round-trip verification for every metric.

Any failed contract is reported in `diagnostic_blockers` and sets
`diagnostic_valid=false`.

`bench/build.rs` embeds Cargo's actual `PROFILE` and `OPT_LEVEL` into the
benchmark binary as `RHIZA_BENCH_CARGO_PROFILE` and
`RHIZA_BENCH_CARGO_OPT_LEVEL`. The JSON environment reports these values as
`cargo_profile` and `cargo_opt_level`. The harness treats a build as an
optimized release only when the profile is exactly `release` and the
optimization level is `1`, `2`, `3`, `s`, or `z`; it does not infer this from
debug assertions. Any other combination is a diagnostic blocker.

The candidates are rotated with `--candidate-order-offset`: offset 0 runs
Postcard then rkyv (AB), while offset 1 runs rkyv then Postcard (BA). Every cell
uses 1,000 warmup iterations and 10,000 measured iterations per phase.

Postcard is the `1.1.3` version resolved in `bench/Cargo.lock`. rkyv is pinned
exactly as:

```toml
rkyv = { version = "=0.8.17", default-features = false, features = [
  "std",
  "bytecheck",
  "little_endian",
  "pointer_width_32",
] }
```

The rkyv decode path copies the network-style `&[u8]` into
`rkyv::util::AlignedVec`, performs checked structural validation through
`rkyv::from_bytes`, and materializes an owned `RequestFrame` or
`ResponseFrame`. The measured path therefore includes alignment copy,
validation, and full owned codec materialization; it is not a zero-copy claim.

This benchmark predates the production wire version 6/schema version 2
redesign. Production now uses a fixed 28-byte envelope plus an
operation-specific archive body rather than the giant request/response enum
measured here. Identity requests and successful Store/Install acknowledgements
therefore use a 28-byte envelope with no body instead of the old 290/698-byte
floor. The codec CPU result remains useful, but the serialized-size rows below
are not the current production wire contract.

## Local release results

The table reports the median `encode_decode_total` result for the
`record.request` cells across three order-rotated AB/BA pairs, six standalone
release runs in total. Each codec therefore contributes six observations per
payload. Every run used 1,000 warmup iterations and 10,000 measured iterations
per phase per cell. All six runs reported Cargo profile `release`, optimization
level `3`, and `diagnostic_valid=true`. Throughput is codec operations per
second.

| Record payload | Codec | Serialized bytes | Median total | Median ops/s | rkyv comparison |
|---:|:---|---:|---:|---:|:---|
| 0 B | Postcard | 188 B | 477.33955 ns | 2,094,945.41 | baseline |
| 0 B | rkyv | 280 B | 175.34995 ns | 5,702,938.54 | 2.7222× throughput; 63.2652% lower latency; 92 B / 48.9362% larger |
| 128 B | Postcard | 317 B | 659.76045 ns | 1,515,707.29 | baseline |
| 128 B | rkyv | 408 B | 232.34375 ns | 4,304,185.33 | 2.8397× throughput; 64.7836% lower latency; 91 B / 28.7066% larger |
| 4096 B | Postcard | 4285 B | 4569.58745 ns | 218,838.14 | baseline |
| 4096 B | rkyv | 4376 B | 432.025 ns | 2,314,973.61 | 10.5785× throughput; 90.5456% lower latency; 91 B / 2.1237% larger |

Ratios use the reported median ops/s values. The reciprocal latency ratios are
2.7222×, 2.8396×, and 10.5771× respectively; their small differences from the
throughput ratios come from the displayed decimal precision.

The release build initially stopped because the local disk was full. Recovery
removed only generated benchmark development artifacts, totaling 1.8 GiB; no
source or user data was removed. The release measurements above were collected
after that cleanup.

## Interpretation and limits

This is a codec CPU diagnostic. It measures operations per second, not
commits per second. It excludes transport framing and I/O, authentication, the
private `rhiza-node` adapter and domain conversion, quorum work, persistence,
`fsync`, the Recorder/application-state materializer, and remote network
effects. The owned value materialization inside the measured codec decode path
is included, as described above.

All six standalone binary reports intentionally set `comparison_valid=false`.
Their blockers state that a single raw run requires alternating paired
repetitions and that codec-only evidence cannot promote production.
`diagnostic_valid=true` proves the harness contracts for a run; it does not
override `comparison_valid`. Aggregating the six local runs reduces
candidate-order noise, but this aggregation remains diagnostic only. It is not
production-promotion evidence.

The private `rhiza-node` rkyv adapter was subsequently redesigned and measured
against Postcard with a fail-closed, schema-3 controlled runner. Wire inflation
was fixed. An initial broad run was invalidated by rare saturated `record`
errors, and later broad/focused runs failed AA/BB drift controls. A stricter
single-cell stability protocol then produced error-free, primary-control-valid
diagnostic advantages for 4096-byte `record` and `fetch_command` at concurrency
4. Those results still need a clean exact snapshot and a different seed/day
before a publishable general performance claim. See
[`recorder-transport-benchmark-2026-07-25.md`](recorder-transport-benchmark-2026-07-25.md).
The real durable three-node workload and physical two-host run remain open.

Production promotion requires all of the following:

- at least 10% higher commits/s;
- no more than 5% p99 latency regression;
- zero request errors;
- identical recovery and checkpoints, together with identical applied state
  and durable log outcomes;
- all eight operations, malformed-input handling, pinned-format compatibility,
  and the existing framing, authentication, fencing, request-id,
  operation-match, overload, and deadline contracts preserved;
- Postcard retained as an explicit rollback path for one release.

These criteria describe a publishable performance-based superiority claim.
They remain open even though the user explicitly accepted the bounded evidence
and changed the operational default to plaintext `tcp-rkyv`.

## Reproduction

Run the harness tests:

```sh
cargo test \
  --manifest-path bench/Cargo.toml \
  --bin rhiza-recorder-codec
```

Build the release binary:

```sh
cargo build \
  --release \
  --manifest-path bench/Cargo.toml \
  --bin rhiza-recorder-codec
```

Capture three AB/BA pairs:

```sh
result_dir="$(mktemp -d)"
for pair in 1 2 3; do
  cargo run --quiet \
    --release \
    --manifest-path bench/Cargo.toml \
    --bin rhiza-recorder-codec \
    -- --warmup 1000 --iterations 10000 --candidate-order-offset 0 \
    > "${result_dir}/pair-${pair}-ab.json"

  cargo run --quiet \
    --release \
    --manifest-path bench/Cargo.toml \
    --bin rhiza-recorder-codec \
    -- --warmup 1000 --iterations 10000 --candidate-order-offset 1 \
    > "${result_dir}/pair-${pair}-ba.json"
done
printf 'results: %s\n' "${result_dir}"
```

For the table above, select `cell_id` values beginning with
`record.request.command_payload_` and compute the median of
`encode_decode_total.latency_ns_per_operation` and
`encode_decode_total.throughput_operations_per_second` across the six reports
for each codec and payload size. Preserve the original JSON reports with the
machine, OS, compiler, Git state, invocation, and candidate order metadata when
using results as diagnostic evidence.
