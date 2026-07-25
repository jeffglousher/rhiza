# rhiza-quepaxa

`rhiza-quepaxa` is the transport-independent QuePaxa consensus engine used
by rhiza. It provides the proposer, recorder state machine, decision proofs,
fixed and stoppable membership transitions, and a file-backed recorder for
embedding and tests. It does not depend on SQLite, HTTP, Tokio, object storage,
or Kubernetes.

The crate deliberately re-exports the `rhiza-core` model types used by its
public API. Applications normally need only this crate to construct commands,
drive consensus, and inspect decisions.

```rust
use rhiza_quepaxa::{Command, CommandKind, Consensus, ThreeNodeConsensus};

let base = std::env::temp_dir().join(format!("rhiza-quepaxa-readme-{}", std::process::id()));
let _ = std::fs::remove_dir_all(&base);
let roots = [base.join("n1"), base.join("n2"), base.join("n3")];
let consensus = ThreeNodeConsensus::new("cluster", "n1", 1, 1, roots)?;
let entry = consensus.propose(Command::new(
    CommandKind::Deterministic,
    b"deterministic command".to_vec(),
))?;
assert_eq!(entry.index, 1);
# Ok::<(), rhiza_quepaxa::Error>(())
```

See `examples/local_three_node.rs` for a complete runnable example.

## Runtime contract

- `RecorderRpc` implementations must enforce a finite deadline for every
  network or process-bound operation. The consensus engine performs quorum RPCs
  concurrently and may return after a quorum while slower calls finish.
- An ordinary command's proposal response does not wait for or require proof
  dissemination. Its decision is offered to one fixed proof worker per
  recorder. Each worker has room for one queued job in addition to its in-flight
  RPC, stores the verified command bytes before idempotently installing the
  proof, and drops new work when that bounded queue is full or disconnected.
  These best-effort failures never change proposal success; sustained
  saturation can therefore leave a recorder without a proof cache until later
  recovery or dissemination.
- Configuration-change decisions, and decisions reached after a transition was
  observed, still install their proof on a recorder quorum synchronously before
  the proposal succeeds.
- Dropping `ThreeNodeConsensus` never waits indefinitely for outstanding RPC
  workers, including proof workers. A transport that ignores its deadline can
  leak its own worker and resources, but cannot block consensus destruction.
- Call `finish_pending_rpcs` with an application-selected bound before removing
  local recorder storage or shutting down a transport used by accepted record,
  proof, or control jobs. The drain does not recover jobs already dropped
  because a bounded worker queue was full.
- Each recorder has one record worker and one control worker, each with room for
  one queued job. Record saturation returns retryable `Pending`; control
  saturation may surface retryable `NoQuorum` or `Unavailable`.
- `register_command` is fallible: it rejects mismatched command hashes and
  succeeds only after a recorder quorum stores the command. `NoQuorum` is
  retryable.
- `PrioritySource` is injectable for deterministic simulation. The default uses
  the operating system random source through `getrandom` and supports all
  platforms supported by that crate.

## Recorder durability

Normal records are acknowledged from a threshold-checkpointed, checksummed append-only
`recorder.wal`. Each frame carries its generation and sequence, the previous
frame digest, the exact slot/configuration/head state, and an optional inline
command. Recovery replays only the continuous digest chain. Fully present
frames with checksum, digest-chain, generation, or sequence corruption fail
closed. An incomplete final frame is treated as an unacknowledged torn tail and
truncated. QWAL v1 cannot distinguish a genuinely torn final frame from a
corrupted declared frame length that extends beyond EOF; that ambiguity remains
an explicit residual format risk until the framing format changes.
Before each append, the recorder evaluates the WAL's 16 MiB byte threshold and
1,024-frame threshold. Because the check precedes the append, an individual
frame can carry the WAL past the 16 MiB soft threshold; the recorder checkpoints
before the following append. Command payloads have no separate hard size bound.
The existing checkpoint format and 1,024-frame boundary are intentionally
retained. A broader crash-safe checkpoint redesign is deferred; the logical
boundary diagnostic below does not replace the physical power-loss deployment
gate.

The steady path acknowledges only after the appended frame is durable. On
Linux it uses `File::sync_data` (`fdatasync`); other platforms conservatively
retain `File::sync_all`. Operations that change WAL metadata, including
checkpoint/rotation truncate and recovery tail repair, always use
`File::sync_all`. Checkpointing first durably replaces command, slot,
configuration, and recorded-head files and only then truncates and fully syncs
the stable WAL inode. Structural configuration changes drain the WAL and keep
their separate crash-recovery intent protocol.

These rules preserve the ordering contract: write frame, sync successfully,
publish the new in-memory Recorder state, then ACK. API-level recovery and
fault-injection tests cover this order. A physical power-loss matrix on ext4,
XFS, and the intended Kubernetes CSI remains a separate deployment gate.

### Recorder WAL sync benchmark

`recorder_sync_bench` measures the actual `RecorderFileStore::record_proposal`
steady WAL append and acknowledgement path without pulling in a rhiza backend
or network transport. A default steady-state run is deliberately capped below
the WAL checkpoint boundary; `--checkpoint-diagnostic` is the explicit
boundary-crossing exception. Each run emits one JSON object with throughput,
successful-call latency percentiles, error count, exact WAL byte/frame
observations, and platform metadata. Every operation uses an equal-sized but
distinct command payload and hash; `--payload-bytes` is the exact payload size
and must be at least 2. All commands and requests are constructed before timing.

The default `--command-mode inline` includes the inline command and its WAL
persistence in every timed `record_proposal` call. `--command-mode pre-stored`
stores every distinct command before warmup and before the timer starts, then
omits commands from measured requests. Command pre-storage is therefore
excluded from its latency and throughput.

```console
cargo run --release -p rhiza-quepaxa --example recorder_sync_bench -- \
  --warmup 100 --operations 500 --label native
```

`--checkpoint-diagnostic` is a boundary-crossing correctness run, not a
steady-state comparison. It forces `--warmup 0 --operations 1025` (and rejects
conflicting explicit values). Operations 1 through 1024 fill generation 1;
operation 1025 measures the synchronous checkpoint before the new proposal is
appended as the first generation-2 frame. The command exits nonzero unless it
observes exactly that one checkpoint, a durable-head generation of 2 through
sequence 1024, and one checksummed generation-2 WAL frame at sequence 1025. It
also drops and reopens the recorder with the expected membership, so production
decoders validate the complete durable head and WAL before success is reported.

```console
cargo run --release -p rhiza-quepaxa --example recorder_sync_bench -- \
  --checkpoint-diagnostic
```

On Linux, `File::sync_data` reaches the normal dynamically linked `fdatasync`
symbol. [`bench/support/fdatasync-as-fsync.c`](../../bench/support/fdatasync-as-fsync.c)
is the comparison shim: it forwards `fdatasync(fd)` to `fsync(fd)` and records
its intercept count at process exit.
[`bench/run-recorder-sync-linux.py`](../../bench/run-recorder-sync-linux.py)
builds the benchmark and shim once, rotates candidate order across balanced
Docker pairs, verifies that the shim observed exactly `warmup + operations`
calls, and preserves raw JSONL plus a summary with commands, hashes, Git state,
and container provenance.

```console
python3 bench/run-recorder-sync-linux.py --pairs 12
```

The tracked 2026-07-17 Docker Desktop Linux/aarch64 results are a historical
diagnostic from an identical-command-per-slot harness. They predate the
distinct-command workload and explicit command-mode methodology documented
above, so they do not validate the current workload. That run used 12 balanced
pairs, each with 100 warmups and 800 measured records. All 19,200 measured
records succeeded. Median throughput was 2,983.9011487711614 ops/s for native
`fdatasync` and 1,911.5215089204817 ops/s with the `fsync` preload. Dividing
the aggregate medians gives 1.561008408666x. However, the median paired
`fsync-preload/native` ratio was 0.9278500671968066, and each candidate won
6/12 pairs. Native/preload median p50 was 240,437.5/398,624.5 ns, p95
793,479/1,239,624.5 ns, and p99
1,603,021/2,123,125 ns. Aggregate throughput and latency favor native, but the
paired result and win split remain mixed. All 12
preload runs observed the expected 900 intercepts, and every run observed 900
WAL frames in generation 1 without a checkpoint.

The historical tracked artifacts are
[`raw.jsonl`](../../docs/benchmarks/recorder-sync-linux-20260717/raw.jsonl)
(24 rows, 49,782 bytes) and
[`summary.json`](../../docs/benchmarks/recorder-sync-linux-20260717/summary.json)
(9,603 bytes). The summary records exact commands, hashes, dirty Git state, and
container provenance. The QuePaxa source SHA-256 is
`54ca511bd8be35e1b2deeb50a1f8f9ced66bb336194e4d7ba07c4473a9d60c1d`
and the benchmark binary SHA-256 is
`7bc075b29e7d49524ea51555b5cc95a0f6d1eea4b9eccff7d634caa27893459d`.
The historical runner SHA-256 recorded by those artifacts is
`bbe7d010c56fae73cc2d65d252093e2e547b4c191a8e14c9ccd7aa7454ed0b7d`
and is retained only for historical provenance; it is not claimed to match the
current runner. The artifacts record fresh build provenance under
`target/recorder-sync-linux-build-final-v3-20260717` and record that the runner's
full-reuse gate verified it. The summary sets `production_valid=false`:
measurements from a dirty tree remain diagnostic, and Docker Desktop's virtual
filesystem cannot reproduce host power loss or the target CSI flush path.
Linux `sync_data` remains a correctness-preserving candidate implementation of
the smaller durability syscall. The aggregate Docker result is favorable, but
paired performance is inconclusive and is not a production speedup claim.
Production performance adoption requires clean physical crash/reopen and
throughput/latency testing on the target ext4/XFS/CSI stack.

## Current-format policy

The minimum supported Rust version is 1.89. This crate is unreleased and makes
no API, persisted-layout, or mixed-binary compatibility promise. Recorder
persistence and decision proofs accept only the exact current format. Their
fixed magic and format identity are corruption and type-safety fences, not
migration negotiation. HTTP or other wire protocols belong to the embedding
application and are not part of this crate.
