# rhiza sql Failover and Throughput Verification

Date: 2026-07-12

Evidence status: the figures below are historical observations from unversioned
local runs. Their `target/rhiza-bench/...` directories are not included in
this repository, so a checkout cannot independently audit them. Re-run the
benchmark to produce versioned evidence before using the figures for a release
or performance claim. New vind artifacts support such claims only when
`artifacts.json.provenance.publishable` is `true`, which requires a successful
run, no failed evidence collection, and verified cleanup plus clean Git source
and immutable benchmark binary and runtime container identities.

## Scope

This change implements four related improvements without making rhiza sql depend
on Kubernetes:

1. Separate peer and client Services, preferred-first multi-endpoint client
   fallback/hedging, and a preferred-proposer deletion benchmark.
2. Command piggybacking on typed QuePaxa Record messages and quorum-early
   `ConfigChange` proof installation.
3. A bounded writer microbatch (maximum 8 requests, 500 microseconds by
   default) for KV and arbitrary SQL writes.
4. An append-only qlog open segment, SQLite WAL/NORMAL cache mode, and a
   renewable cached OSS publisher session with grouped flushes.

RustFS is used only as the local S3-compatible OSS simulator. No RustFS failure
was injected.

## Correctness boundaries

- Endpoint order preserves the preferred node first. Later endpoints are used
  only for fallback or hedge attempts, and every attempt reuses the exact
  request body and `request_id`.
- A recorder validates the piggybacked command against the complete accepted
  value, persists it, and only then acknowledges ISR progress.
- In the implementation measured by this historical note, an ordinary SQL/KV
  FastPath returned after one phase-0 recorder quorum and only configuration
  changes synchronously installed a proof quorum. That acknowledgement contract
  was superseded by `bcd14d4` (`fix(quepaxa): durably fence fast path decisions`):
  the current runtime forms the FastPath proof after phase 0, then durably
  installs it on a recorder quorum on the normal success path. An ambiguous
  installer result is acknowledged only if bounded inspection certifies the
  exact matching committed value. These figures must not be presented as
  measurements of the current FastPath acknowledgement path. Quorum-early
  completion still excludes a slow minority when a quorum is available.
- A microbatch is one consensus entry and one SQLite transaction. Each member
  keeps an independent persistent idempotency record and SQL result, including
  `RETURNING` rows.
- If one SQL request cannot be prepared with the others, requests fall back to
  independent execution instead of failing unrelated clients.
- SQLite is a rebuildable local cache. WAL/NORMAL is safe because the local
  qlog remains the replay source, while OSS remains the disaster-recovery
  source according to the selected durability mode.
- Publisher state is cached only as an optimization. Manifest CAS remains the
  authority, and stale sessions reload after a precondition conflict.

## Performance

Workload: one parameterized INSERT per request, concurrency 4, periodic OSS
durability at 1 second, RustFS metering enabled, resource sampling disabled.

| Run | Duration | Success/error | Writes/s | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|
| Previous periodic baseline | 20 s | 3,251 / 0 | 162.55 | 25.6 ms | 51.2 ms | 51.2 ms |
| Optimized preferred endpoint | 30 s | 8,578 / 0 | 285.93 | 12.8 ms | 25.6 ms | 102.4 ms |
| Three endpoints, before preferred deletion | 10 s | 1,738 / 0 | 173.80 | 25.6 ms | 102.4 ms | 102.4 ms |
| Three endpoints, preferred deleted | 20.00 s | 3,515 / 0 | 175.71 | 25.6 ms | 102.4 ms | 102.4 ms |
| Final image, preferred-first multi-endpoint | 30 s | 9,143 / 0 | 304.77 | 12.8 ms | 51.2 ms | 102.4 ms |

The final image used three endpoints in preferred-first order; it did not
deliberately distribute requests across non-preferred proposers. Its fault
windows were:

| Window | Duration | Success/error | Writes/s |
|---|---:|---:|---:|
| Before preferred pod deletion | 10 s | 3,174 / 0 | 317.4 |
| During deletion/restore | 20.009 s | 5,969 / 0 | 298.316 |

Throughput during the deletion/restore window was about 6.0% below the
pre-deletion window, with zero request errors. The fault command succeeded in
34.846 seconds. The measurement ended with the fault window, so this run has no
separate after-recovery throughput window and does not isolate protocol cost
from routing, restoration, or other system overhead.

These measurements compare two rhiza sql configurations and do **not** establish
Raft-equivalent performance. The paper-conformance and benchmark-comparability
boundaries are documented in
[`quepaxa-paper-conformance-2026-07-12.md`](quepaxa-paper-conformance-2026-07-12.md).

The local run notes report no checkpoint-drain wait: qlog and checkpoint both
ended at index 5193 with the exact same hash. This is an unversioned observation,
not independently auditable evidence or a general recovery-time claim.

Original local run directories (not distributed with this repository):

- Final preferred-first deletion run: `target/rhiza-bench/20260712-113825-51624`
- Normal: `target/rhiza-bench/20260712-034534-80063`
- Preferred deletion, final: `target/rhiza-bench/20260712-035609-39970`
- Longer preferred deletion sample: `target/rhiza-bench/20260712-034903-7352`

## OSS usage and cost

The local notes for the final preferred-first deletion run report 5,259 metered
OSS requests and 29,855,178 retained bytes in 286 objects.

For historical context, the earlier optimized normal run used 3,997 metered OSS
calls for 10,053 warmup and measured writes and retained 29.50 MB in 292 objects.
Its reported request-rate estimate was $1.01 per million writes before retained
storage and network egress, versus the previous $4.53 estimate. Those cost
figures were not recomputed from a repository-retained artifact.

The final access log included transient PUT 412 responses from expected CAS
races and PUT 502 responses that were retried successfully. These were
observations, not injected RustFS failures; all client writes succeeded and the
final checkpoint drain matched the qlog index and hash exactly.

## Verification

- `cargo test --workspace --all-targets`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo fmt --all -- --check`
- `cargo test --manifest-path bench/Cargo.toml`
- `cargo clippy --manifest-path bench/Cargo.toml --all-targets -- -D warnings`
- `shellcheck scripts/*.sh`
- `scripts/check-deploy.sh`
- release Docker image build
- vind/RustFS normal and preferred-proposer deletion benchmarks
