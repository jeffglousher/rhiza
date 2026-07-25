# RecorderRpc adoption gate (2026-07-15)

> **Superseded on 2026-07-25:** This document preserves the decision and
> evidence available on 2026-07-15. The current decision promotes plaintext
> `tcp-rkyv` as the exclusive default; `http` and `tcp-postcard` are explicit
> rollback/diagnostic modes. See
> [`rkyv-recorder-path-2026-07-25.md`](rkyv-recorder-path-2026-07-25.md).

## Decision

Keep HTTP/JSON as the production default. `tcp-postcard` is available as an
opt-in transport for a single trusted, isolated Kubernetes cluster. Setting
`RHIZA_RECORDER_TLS=on` adds TLS 1.3 server authentication to the same framed
Postcard protocol so the two TCP modes can be compared without changing the
RecorderRpc semantics. Neither TCP mode is promoted by the existing
dirty-source, single-host diagnostics.

Recorder TLS is not required by the current trusted-cluster model and remains a
comparison candidate rather than a performance gate. It protects RecorderRpc
only: the public API and log-fetch paths retain their separately configured HTTP
contract. If the trust boundary later includes untrusted workloads, nodes,
networks, or multiple clusters, evaluate cluster-wide authentication such as
service-mesh mTLS or WireGuard instead of treating server-only Recorder TLS as
complete cluster security.

## Current transport contract

- `RHIZA_RECORDER_TRANSPORT=tcp-postcard` with `RHIZA_RECORDER_TLS=off` is
  plaintext. Switching `RHIZA_RECORDER_TLS=on` requires a CA bundle, server
  certificate, private key, and an exact `recorder_tls_server_name` for every
  member. The modes never fall back to each other.
- TLS accepts only `on|off`, cannot be enabled for HTTP, and rejects TLS files,
  server names, or Secrets while off. The legacy `tcp-tls-postcard` transport
  value is rejected so dual configuration cannot silently choose a mode.
- TLS is restricted to TLS 1.3 with the `rhiza-recorder/1` ALPN and 0-RTT
  disabled. It authenticates the server certificate and DNS name; callers are
  authenticated by the encrypted HELLO peer-token exchange rather than a client
  certificate.
- Framed Postcard carries the seven typed `RecorderRpc` operations over bounded
  persistent connection pools. HELLO node/token/generation fields provide
  identity and fencing checks, not cryptographic authentication.
- Selecting TCP is exclusive: Pods listen on recorder port 8082 and do not
  listen on the HTTP recorder port 8081. Restarting with the default selector
  restores HTTP.
- The shipped Kubernetes renderer uses exact headless-Service DNS names. The
  deployment check rejects Ingress, Gateway, NodePort, LoadBalancer, hostPort,
  hostNetwork, and externalIPs exposure.
- The renderer mounts one Secret across the StatefulSet. Its certificate SAN
  must therefore cover every ordinal headless-Service DNS name in the active
  three-to-seven-node configuration.
- The listener still binds to `0.0.0.0:8082`; isolation therefore depends on a
  trusted cluster plus CNI/NetworkPolicy enforcement. The repository manifests
  check exposure but do not create a default-deny NetworkPolicy.
- Server connection and admitted-operation counts are bounded. Completed tasks
  are reaped, shutdown waits for admitted mutations, failed pooled connections
  are discarded, and a request is never automatically replayed after a write
  begins.

## Validation completed

- `cargo test -p rhiza-node -p rhiza-cli --all-features`: 223 tests passed,
  including SQL, graph, KV, durability, recovery, checkpoint restore, and six
  Recorder TCP integration tests.
- `cargo clippy -p rhiza-node -p rhiza-cli --all-targets --all-features --
  -D warnings`, root and benchmark formatting, deployment/static checks, and
  `git diff --check` passed.
- `cargo test --locked --manifest-path bench/Cargo.toml`: 47 tests passed;
  benchmark all-targets check passed.
- The benchmark histogram stores exact microseconds and reports nearest-rank
  p99.9 (`p99_9_ms`).
- An independent review found no new plaintext-specific P0/P1 runtime defect,
  but retained the deadline, DNS, isolation, and evidence gates below.

## Local transport diagnostic

Three rotated local runs used 40,000 operations per cell, a 4,096-operation
warmup, 128-byte payloads, and concurrency 1/8/64. All cells had zero errors.
The aggregate is stored at
`target/rhiza-bench/local-transport-20260715-rotated-3run/summary.json`.

Against HTTP/JSON, `tcp-postcard` delivered 2.94x/2.17x/1.99x throughput at
concurrency 1/8/64. Its p99 improved by approximately 51%/74%/75%.
Against the warmed persistent TLS control, plaintext TCP throughput stayed
within about +/-4% in every cell and high-concurrency tails were effectively
equal. This does not show a performance reason either to require or remove TLS.

The local aggregate is diagnostic only: raw per-run reports, order, and full
provenance were not preserved.

## Latest Vind Kubernetes A/B diagnostic

The same SQL-only image and benchmark client were used in fresh isolated
three-node Vind clusters with sync durability, a 10-second warmup, a 60-second
write measurement, and concurrency 4. Resource sampling and object metering
were enabled. Both runs verified three runtime instances, checkpoint equality,
and successful namespace/vcluster cleanup.

| Recorder transport | Commits | Errors | Commits/s | p50 | p95 | p99 | p99.9 | Rhiza CPU |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| HTTP/JSON | 6,213 | 4 HTTP 503 | 103.55 | 20.109 ms | 132.154 ms | 246.907 ms | 749.083 ms | 35.978 CPU-s |
| TCP/Postcard | 9,329 | 0 | 155.48 | 22.678 ms | 35.216 ms | 109.761 ms | 274.496 ms | 34.538 CPU-s |

TCP changed throughput by **+50.2%**, p95 by **-73.4%**, p99 by **-55.5%**,
p99.9 by **-63.4%**, and measured Rhiza CPU by **-4.0%**. Its p50 was **12.8%
worse**. TCP average Rhiza memory was lower, while its peak was 27.1% higher;
one pair is insufficient to classify that peak as a transport cost.

HTTP artifacts are in `target/rhiza-bench/20260715-151727-67528`; TCP artifacts
are in `target/rhiza-bench/20260715-152045-86353`. The comparison summary is
`target/rhiza-bench/vind-http-vs-tcp-postcard-20260715.json`.

These artifacts are intentionally non-publishable because the source tree was
dirty and the harness compares its manifest-list image ID with the runtime
config digest. The actual runtime digest and benchmark-client SHA-256 matched
between the two runs, so the pair is useful for development evaluation but not
for final adoption evidence.

## Historical TLS Vind observation

Before the plaintext decision, one 20-second SQL-only Vind pair observed
139.9 commits/s for HTTP and 177.0 commits/s for TCP/TLS/Postcard, both with
zero errors and matching checkpoints. Those artifacts remain at
`target/rhiza-bench/20260715-113643-63526` and
`target/rhiza-bench/20260715-120905-11241`. They are historical diagnostics,
not the current runtime contract.

## Remaining adoption gates

1. Run at least three order-rotated HTTP/TCP pairs for concurrency 1, 8, and 64
   from a clean revision, preserving raw reports and identical image provenance.
2. Repeat on two explicitly approved physical hosts with both 2+1 placements;
   record RTT/loss, throughput, p50/p95/p99/p99.9, CPU/op, memory, reconnect,
   checkpoint/materialization equality, and a 30-minute soak.
3. Require zero correctness, qlog, checkpoint, or materialization divergence
   and no unexplained transport errors.
4. Enforce the request deadline during server dispatch. The current server only
   rejects a zero `remaining_deadline_ms`; a queued mutation may execute after
   the caller's deadline and become an ambiguous late commit.
5. Bound or replace blocking `to_socket_addrs()` resolution, which currently
   occurs outside the async CALL deadline during reconnect.
6. Add operational metrics for pool saturation, reconnects, rejected HELLOs,
   overload, deadline expiry, and ambiguous outcomes.
7. Add an enforced default-deny NetworkPolicy/CNI contract before describing
   plaintext as isolated in production documentation.

Promotion should require concurrency-1 p99 to improve by at least 15%, with
concurrency-64 p99.9 and CPU/op no worse than HTTP. Until all gates pass,
`tcp-postcard` remains opt-in and HTTP remains the rollback/default path.

## Residual contract

A client timeout after a mutating write can still represent an ambiguous late
commit; the connection is closed and the request is not replayed. DNS is
refreshed for each new connection, but the system resolver is not independently
bounded. HELLO tokens are configuration secrets for fencing only and must not be
presented as authentication. These constraints are explicit adoption blockers,
not benchmark noise.
