# Physical power-loss and equal-durability gate

A process `kill`, container restart, VM process stop, or pod deletion is not
physical power loss. Those operations can retain host caches, perform ordered
teardown, or leave the storage controller powered. They belong only in a
logical-crash diagnostic league.

This repository provides a fail-closed envelope around an external disposable
lab. It does not provide a power-loss controller and does not claim that
self-authored JSON booleans prove physical behavior. Configuration JSON files
are declarations. Evidence is the timestamped stdout/stderr/exit status emitted
live by the configured provider, workload, atomic barrier/cut, reboot, and verification
hooks, bound to a controller identity and exact target.

## Commands and safety boundary

```sh
scripts/bench-power-loss-durability.sh plan target/power-loss-plan.json
scripts/bench-power-loss-durability.sh validate-live
POWER_LOSS_DURABILITY_OPT_IN=I_UNDERSTAND_DESTRUCTIVE_POWER_LOSS \
  scripts/bench-power-loss-durability.sh provider-run-envelope target/power-loss-run
scripts/bench-power-loss-durability.sh validate-artifacts target/power-loss-run
```

`plan` and `validate-artifacts` are pure with respect to block devices.
`validate-live` performs read-only host inspection and executes no hooks. It
requires Linux, an exact canonical `POWER_LOSS_TARGET=/dev/...` matching the
provider declaration, ext4 or XFS reported by `lsblk`, and absolute,
non-symlinked executable hooks. It refuses a target or child that is mounted,
backs `/`, backs active swap, or has kernel holders. It never infers a target.
CSI declarations must name the driver and are retained with the artifacts.

`provider-run-envelope` is destructive and requires the exact opt-in string.
Use it only from an out-of-band controller against a disposable device or VM.
For both `pre-ack` and `post-ack`, it captures these stages in order:

1. live provider/target validation;
2. typed unique-ID workload preparation, without starting either request;
3. one external-controller request-barrier and physical-power-cut operation;
4. reboot and service restart;
5. final ID lookup and correctness/RPO/RTO verification.

The five standalone executable hooks are
`POWER_LOSS_PROVIDER_VALIDATE_HOOK`, `POWER_LOSS_WORKLOAD_PREPARE_HOOK`,
`POWER_LOSS_CUT_AT_BARRIER_HOOK`, `POWER_LOSS_PROVIDER_REBOOT_HOOK`, and
`POWER_LOSS_POST_REBOOT_VERIFY_HOOK`. Each accepts exactly
`TARGET CUTPOINT TRIAL_DIR`, performs only its named stage, and emits one JSON
object to stdout. They are executed as programs, not sourced into the harness.
There is deliberately no separate cutpoint-observation plus later power-cut
contract: artifacts containing those legacy stages are nonpublishable.

Each hook receives `TARGET CUTPOINT TRIAL_DIR`. Its stdout must be one JSON
machine capture; stderr, exit status, hook path, start/end timestamps, and both
stream hashes are captured alongside it. The live guard resolves every hook to
an absolute canonical path, refuses symlinked path components, hashes the
executable, and checks the original path and hash around snapshot creation.
Before any stage, it copies each validated hook into the run's private
`hooks/` directory, verifies that source bytes did not change while copying,
and executes only that snapshot, checking its hash immediately before and
after execution. Evidence records the original
canonical path, snapshot path, and executable hash; snapshot tampering fails
validation. Captures must agree on controller,
target, provider, filesystem, commits, and images, and must explicitly state
`logical_process_kill: false`. Before invoking the atomic barrier/cut hook, the envelope
requires the live provider capture to corroborate the exact safe target and
requires matching system-bound prepared IDs. The barrier/cut hook itself must
start both requests, own and hold their request barriers until power is removed,
and return the controller's
single causal snapshot. The envelope is always written
`publishable: false`; only `validate-artifacts` can report
`publishable-eligible`, and that still does not replace repeated rotated trials.

## Declarations and equal-durability league

`POWER_LOSS_PROVIDER_DECLARATION` names a disposable `block-device` or `vm`,
canonical target, ext4/XFS filesystem, controller identity, exact Rhiza and
Hiqlite commits/images, and optional CSI driver. It describes the intended lab;
live hook captures must corroborate it.

`POWER_LOSS_DURABILITY_DECLARATION` selects Rhiza
`object-authoritative-sync` and Hiqlite `comparable-durable-ack`, declares the
matched RPO, and requires matching fsync, log, and failure/RPO policies. The
post-reboot verifier must independently emit the observed durability fields.
An interval/asynchronous WAL, different failure semantics, missing observation,
or unmatched RPO is a separate nonpublishable league.

## Artifact correctness gate

Each verification capture contains exactly one Rhiza and one Hiqlite ledger
row for its cutpoint. Workload preparation binds each ID to `rhiza` or
`hiqlite`, and that exact mapping must survive through the barrier/cut entries,
ledger, and final lookup. A pre-ACK cutpoint entry must show the request start,
no durable ACK, `barrier_held_until_power_removed: true`, and the controller
invariant `controller-held-durable-ack-path-until-power-removed`. A post-ACK entry
must show the system's exact ACK kind, its durable-ACK timestamp, and a later
`power_removed_at`. Both modes require one controller event ID. A shared
observation flag or a separate later cut is insufficient. IDs must be non-empty and globally unique. Pre-ACK rows
may be present or absent within the declared RPO. Every post-ACK ID must be
present exactly once within the matched RPO. Duplicate occurrence counts,
unresolved `unknown`, post-ACK absence, excessive RPO, invalid RTO, or missing
cut/reboot capture fails validation.

Outcome values are exact: `present` means integer occurrence count `1` and
classification `present`; `absent` means count `0` and classification
`absent-within-declared-rpo`. Fractional counts, arbitrary classifications,
and every other combination fail closed.

`validate-artifacts` also requires:

- an exact manifest of every declaration and raw stdout/stderr/meta capture
  (only the two root files `manifest.json` and `SHA256SUMS` are excluded);
- matching per-file SHA-256 values and `SHA256SUMS` without self-hashing;
- ISO timestamps and successful hook return codes;
- total non-overlapping stage chronology from provider validation through
  preparation, atomic barrier/cut, reboot, and verification;
- a changed boot identity whose transition timestamp is after physical power
  removal;
- identical target/provider/filesystem/controller/commit/image provenance;
- machine-observed equal-durability settings and service/full RTO;
- no logical-kill artifact presented as physical power loss.

The static checker uses explicit command shims to replay canonical-path,
filesystem, mount/child-mount, system-device, swap, holder, and hook-path
rejections without opening or modifying a real block device. The destructive
command refuses this test mode.

A complete validation result is only publication eligibility for one trial.
Publication additionally requires repeated order-rotated trials, preservation
of all attempts, and a comparison report that keeps unmatched durability
leagues separate.
