# Recorder transport benchmark

Date: 2026-07-25

Status: stable single-cell diagnostics complete; evidence accepted for default switch

## Conclusion

The plaintext `tcp-rkyv` adapter is functionally available, and its wire-size
inflation has been fixed. Stable single-cell runs show a strong diagnostic
advantage for 4096-byte `record` and `fetch_command` at concurrency 4, while
identity is neutral. The result is not publishable promotion evidence
because it comes from the same seed and day on a dirty development tree and
some non-primary tail controls failed.

On 2026-07-25, the user accepted those stated limits and selected plaintext
`tcp-rkyv` as the absent-selector, renderer, and deployment default. This
operational decision does not make the measurements publishable or prove
production superiority. `http` and `tcp-postcard` remain explicit rollback and
diagnostic modes. Modes are exclusive: there is no TLS, negotiation, fallback,
dual listener, or mixed-codec pool. Default bundles carry
`recorder_tcp_addr`, workloads listen on port 8082, and a mode change requires
a coordinated stop/restart or an atomic new-configuration cutover—not a
sequential `OnDelete` rollout. Strict HTTP validation rejects the rkyv bundle's
`recorder_tcp_addr`, so HTTP rollback uses a separate immutable address-free
bundle. Non-HTTP manifests expose Recorder TCP 8082 only and omit the Recorder
HTTP 8081 port and environment.

## Wire redesign

rkyv wire version 6 and schema version 2 replace the giant archived
request/response enum with:

- a fixed 28-byte envelope;
- an operation-specific archive body;
- exact marker, version, operation, and body-length checks;
- checked `AlignedVec` archive access and bounded domain conversion.

Identity requests and successful Store/Install acknowledgements now need only
the 28-byte envelope and have a zero-byte archive body. Under wire version 5,
the corresponding giant-enum request and response floors were 290 and 698
bytes. This fixes the structural inflation that confounded the first transport
comparison.

The path still performs checked archive traversal, owned wire deserialization,
and domain conversion. It is not an end-to-end zero-copy claim.

## Controlled runner

Transport benchmark schema version 3 adds fail-closed controls:

- exactly one raw candidate per process;
- concurrent prewarm of both connection leases;
- seeded AB, BA, AA, and BB schedules;
- pair-first aggregation;
- deterministic bootstrap confidence intervals;
- per-pair, per-cell, and per-field validity controls;
- rejection when raw-run validation or same-candidate drift controls fail.

AA/BB are controls for host and time drift, not codec comparisons. A dirty Git
tree also blocks publishability as expected development provenance. That
provenance blocker is separate from performance noise and must not be used to
explain or dismiss measured drift.

## Final measurements

### Full concurrency 1/4/32 run

Aggregate:

```text
/tmp/rhiza-controlled-rkyv-final.a6ADyB/summary.json
```

The raw run is invalid. At concurrency 32, the `record` workload very rarely
hit the 250 ms deadline with `ProposeFailed` for both codecs: Postcard recorded
1 and 11 errors in two affected raw results, rkyv recorded 33, and one warmup
error also occurred. Concurrency 32 is a saturation diagnostic only and
cannot be included in a codec comparison.

The shared failure mode is useful operational evidence, but invalidates
diagnostic, comparison, production, and publication claims for this aggregate.

### Concurrency 1/4 controlled run

Aggregate:

```text
/tmp/rhiza-controlled-rkyv-c1c4.o6CZ9c/summary.json
```

This run is diagnostically and production-valid, but comparison-invalid because
AA/BB same-candidate time drift exceeded its threshold. Its ratios are
illustrative only. In particular, 4096-byte fetch at concurrency 4 reported:

| Field | Median ratio, rkyv/Postcard | Deterministic-bootstrap 95% interval |
|:---|---:|---:|
| Success throughput | 1.1096× | 0.9803×–1.3592× |
| Successful p50 | 0.8870× | 0.7071×–1.0382× |
| Successful p99 | 0.8552× | 0.6679×–0.9496× |

Lower latency ratios are better. Identity and `record` showed no detected
advantage. Because the same-candidate controls failed, none of these ratios is
a promotion result.

### Focused 4096-byte fetch at concurrency 4

Aggregate:

```text
/tmp/rhiza-rkyv-fetch4k-c4.VRWXa7/summary.json
```

This single-cell run is diagnostically valid but control-invalid. It reported
throughput median 1.0395× with interval 0.8964×–1.1278×, and p50 ratio
0.9208× with interval 0.9136×–0.9354×. The repeated p50 direction is worth
retesting, but the invalid controls prevent a speedup claim.

## Stable single-cell rerun

The stability protocol fixed the workload sequence as:

1. identity at concurrency 4;
2. 4096-byte `record` at concurrency 4;
3. 4096-byte `fetch_command` at concurrency 4.

Each workload ran as a single cell in a separate candidate process. Each run
used 10,000 warmup operations, at least 100,000 measured operations and at
least five measured seconds, 11 pairs, seed `20260726`, 50,000 deterministic
bootstrap resamples, and a ±10% same-candidate control-drift threshold. A
three-minute cooldown separated cells. The protocol remained plaintext-only:
no TLS and no codec mixing or negotiation.

Every accepted summary contained 88 raw results for each codec, zero errors,
and an exact match against an independent recomputation.

### Identity at concurrency 4

Summary:

```text
/tmp/rhiza-stable-identity-c4.YZGSxR/summary.json
```

| Primary field | Median ratio, rkyv/Postcard | 95% interval |
|:---|---:|---:|
| Success throughput | 1.000506× | 0.993843×–1.005577× |
| Successful p50 | 0.998765× | 0.996245×–1.001859× |

Identity passed its non-regression gate and is neutral.

### 4096-byte record at concurrency 4

Summary:

```text
/tmp/rhiza-stable-record4k-c4.cjXKDN/summary.json
```

| Field | Median ratio, rkyv/Postcard | 95% interval |
|:---|---:|---:|
| Success throughput | 1.076508× | 1.067069×–1.091701× |
| Successful p50 | 0.923335× | 0.916428×–0.934878× |
| Successful p95 | 0.929091× | 0.902492×–0.946062× |

The preregistered primary controls passed. Successful p99 remains exploratory
because Postcard's p99 same-candidate drift was 13.623%, outside the ±10%
control bound.

### 4096-byte fetch at concurrency 4

The first attempt is retained only as discarded provenance:

```text
/tmp/rhiza-stable-fetch4k-c4.1x432A
```

An external `cargo test`/XProtect event drove primary same-candidate drift as
high as 45.334%. That attempt was discarded before interpretation. After the
host gate passed, the retry summary was:

```text
/tmp/rhiza-stable-fetch4k-c4-retry.3jEk5q/summary.json
```

| Field | Median ratio, rkyv/Postcard | 95% interval |
|:---|---:|---:|
| Success throughput | 1.091948× | 1.081343×–1.107670× |
| Successful p50 | 0.917375× | 0.906322×–0.923902× |
| Successful p95 | 0.934167× | 0.915698×–0.951100× |

Primary and p95 controls passed. Successful p99 is exploratory because both
codecs exceeded the p99 drift threshold.

The summaries still set global `comparison_valid=false` and
`publishable=false`: the dirty development tree is a provenance blocker and
non-primary controls failed. Those global flags do not erase the preregistered
metric-specific primary-gate results, but they prohibit a general promotion or
publication claim.

## Interpretation

The isolated codec benchmark shows a real CPU benefit, and wire version 6
removes the previous giant-enum inflation. The stable protocol detects a strong
diagnostic advantage for the two measured 4096-byte concurrency-4 cells.
Identity remains neutral.

The evidence supports:

- functional plaintext rkyv transport;
- corrected compact wire behavior;
- primary-control-valid 4096-byte `record` throughput and p50 advantages;
- primary-control-valid 4096-byte `fetch_command` throughput and p50
  advantages;
- neutral identity behavior;
- concurrency-32 saturation sensitivity in `record`.

By itself, the benchmark does not support:

- claiming advantage outside the preregistered 4096-byte concurrency-4 cells;
- treating concurrency 32 as valid codec evidence;
- treating p99 as confirmatory;
- publishing or promoting from a same-seed/day dirty-tree result.

The default was nevertheless changed by explicit user decision with these
limits visible. That decision must not be rewritten as a stronger benchmark
claim.

## Rerun recommendation

Confirm the stability protocol from a clean exact source snapshot on a quiet,
CPU-pinned host, using a different seed and day. Preserve the fixed
identity/c4 → record-4KiB/c4 → fetch-4KiB/c4 order, single-cell candidate
processes, 10,000 warmup operations, at least 100,000 measured operations and
five seconds, 11 pairs, 50,000 deterministic-bootstrap resamples, ±10% drift
gate, and three-minute cooldown between cells.

Require all raw, per-pair, per-cell, per-field, and AA/BB controls to pass for
the preregistered primary fields before publishing a general performance or
production-superiority claim. Record host-load and scheduler-stall indicators.
Keep concurrency 32 as a separate saturation stratum unless it completes with
zero errors. Do not confuse a dirty-tree provenance blocker with runtime
performance noise.

Build the release harness with:

```sh
cargo build \
  --release \
  --manifest-path bench/Cargo.toml \
  --bin rhiza-recorder-transport
```

## Limits

These are local single-host transport diagnostics. They do not establish real
QuePaxa commit throughput, persistence cost, remote-network behavior, or
recovery equivalence. A physical two-host run and the real durable three-node
restart comparison—including applied state, qlog, recovery, and checkpoint
parity—remain unproven.
