# Protobuf(Prost) versus Postcard RPC benchmark

Date: 2026-07-17
Status: diagnostic only; Protobuf candidate superseded by rkyv direction

> **Superseded on 2026-07-25:** This document preserves the decision and
> evidence available on 2026-07-17. The current decision promotes plaintext
> `tcp-rkyv` as the exclusive default; `http` and `tcp-postcard` are explicit
> rollback/diagnostic modes. See
> [`rkyv-recorder-path-2026-07-25.md`](rkyv-recorder-path-2026-07-25.md).

## Decision

Keep Postcard as the Recorder RPC production default until a replacement passes
the real-workload adoption gate. The next codec candidate is now rkyv, not
Protobuf; see
[`rkyv-recorder-path-2026-07-25.md`](rkyv-recorder-path-2026-07-25.md).

The synthetic TCP benchmark shows that Prost can be faster for some payload and
concurrency combinations, but the advantage is not stable across runs or
security strata. The benchmark does not exercise the real Recorder operation
enums, domain conversion, QuePaxa quorum, persistence, checkpointing, or
recovery. It is therefore insufficient evidence for replacing the production
codec.

This historical comparison does not reject Protobuf as a format, but Protobuf
is no longer the agreed next Recorder codec path.

## What was compared

`rhiza-transport` now exposes four isolated candidates:

- `tcp-postcard`
- `tcp-prost`
- `tcp-tls-postcard`
- `tcp-tls-prost`

Protobuf is implemented with Prost 0.14.4; Postcard is 1.1.3. Both codecs use
the same `WireRequest` and `WireAck` values and the same surrounding path:

- four-byte big-endian length prefix
- one warmed persistent connection per worker
- `TCP_NODELAY`
- one MiB frame limit
- two-second per-call timeout
- identical payload, request count, warmup, and response validation
- TLS 1.3, the same generated server certificate, and `rhiza-bench/1` ALPN for
  the TLS pair
- no handshake in the measured window

The four candidates rotate through all four execution positions. The runner
compares codecs only inside the same security stratum. It first calculates the
Prost/Postcard ratio inside each run and only then takes the median, avoiding a
winner reversal caused by comparing separately aggregated absolute medians.

The ratio convention is:

- throughput above zero percent favors Prost
- latency below zero percent favors Prost

## Environment and provenance

- Host: Apple M3, macOS 26.3 arm64
- Rust: 1.95.0
- Git commit: `c9cf59024689fb3a30812dbf17ff018dc053fd28`
- Release binary SHA-256:
  `6bf32ade27d4c5136e8e75332fe889a255270618edc121862f4ec93175baab10`
- Warmup: 8,192 calls per cell in the confirmation run
- Measurement: 200,000 calls per cell
- Payloads: 128 and 4,096 bytes
- Concurrency: 1, 8, and 64
- Repetitions: four, with fully balanced candidate order
- Errors: zero in warmup and measurement

The repository contained unrelated in-progress performance work. The report is
therefore intentionally marked `comparison_valid=false` and
`production_valid=false`, despite passing its diagnostic invariants. Results
are stored below `target/rhiza-bench/rpc-codec-20260717-proto-postcard-confirm`.

## Confirmation results

The table reports the median of four within-run Prost/Postcard ratios. Positive
throughput is better for Prost; negative p99 is better for Prost.

| Security | Payload | Concurrency | Prost throughput | Prost p99 |
|---|---:|---:|---:|---:|
| Plaintext | 128 B | 1 | +5.16% | -4.80% |
| Plaintext | 128 B | 8 | +3.47% | -6.53% |
| Plaintext | 128 B | 64 | +14.82% | -23.63% |
| Plaintext | 4,096 B | 1 | +10.53% | -4.16% |
| Plaintext | 4,096 B | 8 | +20.24% | -30.82% |
| Plaintext | 4,096 B | 64 | -24.08% | +173.77% |
| TLS | 128 B | 1 | +3.24% | -9.14% |
| TLS | 128 B | 8 | -9.65% | +51.88% |
| TLS | 128 B | 64 | -3.62% | +13.34% |
| TLS | 4,096 B | 1 | -1.89% | +35.17% |
| TLS | 4,096 B | 8 | +7.56% | -14.80% |
| TLS | 4,096 B | 64 | +8.20% | +12.33% |

Equal-weight geometric means across the six cells were:

| Security | Prost throughput | Prost p99 |
|---|---:|---:|
| Plaintext | +3.93% | +3.56% |
| TLS | +0.44% | +12.46% |

These aggregate numbers are not an adoption score. Run-level ranges were wide.
For example, plaintext 4,096 B/concurrency 64 ranged from -42.72% to +9.54% in
throughput and from -4.07% to +321.02% in p99. The earlier 60,000-call run had
reported the opposite direction for the same cell: +12.1% throughput and
-14.2% p99. This reversal is direct evidence that the loopback host noise is
larger than the codec signal in some cells.

## Wire size

Postcard is consistently four bytes smaller for both request and response in
this schema.

| Message | Postcard | Protobuf(Prost) | Difference |
|---|---:|---:|---:|
| 128 B request | 201 B | 205 B | +4 B |
| 4,096 B request | 4,169 B | 4,173 B | +4 B |
| response | 72 B | 76 B | +4 B |

At 128 B the combined request/response increase is about 2.93%; at 4,096 B it
is about 0.19%. The size difference is small relative to transport and
consensus costs, but Postcard is the compact winner for the tested schema.

## Engineering trade-offs

Postcard fits the current private Rust-to-Rust RPC well. It reuses the Serde
model, has a small wire representation, and requires no schema generation or
domain conversion layer. Its main cost is tighter coupling to Rust types and a
weaker cross-language/versioning story.

Protobuf provides an explicit schema, field-number-based evolution, and broad
cross-language tooling. In rhiza it would also add a private wire model and
checked conversion for all Recorder operations. Prost decoding discards unknown
fields, duplicate singular/oneof values use Protobuf semantics, and byte arrays
that represent hashes or fixed-width integers must be validated explicitly.
Those semantic differences are absent from this flat synthetic message.

QuePaxa domain types must remain codec-independent. Do not add Prost derives to
`rhiza-quepaxa`, and never use Protobuf bytes as a canonical hash, configuration
digest, qlog identity, or decision identity. A future Protobuf adapter belongs
inside the private `rhiza-node` transport boundary and must fail closed on codec
mismatch and invalid domain conversion.

## Adoption gate

The rkyv candidate should replace Postcard only after all of the following pass:

1. Implement a private candidate mirror of all eight Recorder operations with
   bounded frames and checked domain conversion, without changing QuePaxa.
2. Run a clean, CPU-isolated codec benchmark with at least 5-10 seconds per cell
   and multiple fully balanced order cycles.
3. Run the real durable three-node QuePaxa workload and a physical two-host
   benchmark using the same workload and persistence settings.
4. Require at least 10% higher commits/second, no more than 5% p99 regression,
   zero request errors, and identical applied state and checkpoints.
5. Verify restart and recovery equality before changing the default.

Until the rkyv gate passes, the production result is **HOLD: keep Postcard**.
