# rhiza-quepaxa

`rhiza-quepaxa` is the transport-independent QuePaxa consensus engine used
by rhiza. It provides the proposer, recorder state machine, decision proofs,
fixed and stoppable membership transitions, and a file-backed recorder for
embedding and tests. It does not depend on SQLite, HTTP, Tokio, object storage,
or Kubernetes in its normal production dependency closure. Its test suite also
checks SQL-produced QEFX payload interoperability through a test-only
`rhiza-sql` dependency.

The crate deliberately re-exports the `rhiza-core` model types used by its
public API. Applications normally need only this crate to construct commands,
drive consensus, and inspect decisions.

```rust
use rhiza_quepaxa::{Command, CommandKind, RecorderRpcContext, ThreeNodeConsensus};

let base = std::env::temp_dir().join(format!("rhiza-quepaxa-readme-{}", std::process::id()));
let _ = std::fs::remove_dir_all(&base);
let roots = [base.join("n1"), base.join("n2"), base.join("n3")];
let consensus = ThreeNodeConsensus::new("cluster", "n1", 1, 1, roots)?;
let entry = consensus.propose(RecorderRpcContext::default_timeout(), Command::new(
    CommandKind::Deterministic,
    b"deterministic command".to_vec(),
))?;
assert_eq!(entry.index, 1);
# Ok::<(), rhiza_quepaxa::Error>(())
```

See `examples/local_three_node.rs` for a complete runnable example.

## Runtime contract

- Every recorder operation receives a `RecorderRpcContext`, which carries an
  absolute deadline and cancellation signal. Network or process-bound
  implementations must use both before starting I/O and while waiting for it.
  Every public consensus operation accepts a caller-owned context; callers may
  choose the five-second `default_timeout` explicitly. Expiry of a mutating call
  is reported as `UnknownOutcome`, not as a safe-to-retry failure.
- A phase-0 recorder quorum forms an ordinary FastPath decision proof after one
  recorder round. That protocol decision point is not the public acknowledgement
  boundary: both FastPath and Phase2 proofs are installed durably on a recorder
  quorum on the normal success path. If installation reports an ambiguous
  failure, the API may still acknowledge only after a bounded inspection
  certifies the exact matching committed value. Otherwise it returns
  `UnknownOutcome`; conflicting safety evidence remains a terminal error.
- Recovery may reconstruct a FastPath decision from its recorder summaries.
  This does not create an unconditional live acknowledgement shortcut; after an
  ambiguous installer result, it can contribute only to bounded exact-value
  reconciliation. Recovery never reconstructs a Phase2 decision from summaries
  alone; Phase2 recovery requires a durably installed decision proof.
- Dropping `ThreeNodeConsensus` cancels and detaches outstanding RPC workers;
  it never waits for them. A transport that ignores cancellation can leak its
  own worker and resources, but cannot block consensus destruction.
- `finish_pending_rpcs` is a diagnostic observation only. It is never proof
  that a shutdown is quiescent: it observes every call sharing the consensus
  instance, including concurrently admitted work owned by other callers.
  Before closing recorder storage or transport, an owner must close its own
  admission and await or drain only its own calls. The diagnostic does not
  recover jobs already dropped because a bounded worker queue was full.
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
Before each append, the recorder evaluates the WAL's 64 MiB byte threshold and
1,024-frame threshold. Because the check precedes the append, one accepted
command (at most 512 KiB) can carry the WAL past the 64 MiB soft threshold; the
recorder checkpoints before the following append. Commands larger than 512 KiB
are rejected before they enter the durable Recorder protocol.
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

The minimum supported Rust version is 1.89. Recorder persistence and decision
proofs accept only their exact current format; adjacent N/N+1 and mixed-binary
compatibility are not implemented. Their fixed magic and format identity are
corruption and type-safety fences, not migration negotiation. The canonical
artifact inventory and design-only release policy are in the workspace
[persisted-format baseline](../../docs/storage-format-compatibility.md); HTTP
or other wire protocols belong to the embedding application.
