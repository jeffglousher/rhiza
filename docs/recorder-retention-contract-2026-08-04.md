# Recorder retention contract

## Status, scope, and non-goals

This is an approved **design contract**, not an implementation or a persisted
format authorization. It defines the minimum semantics a future recorder
retention change must prove before it can replace the current full checkpoint
rewrite in `crates/rhiza-quepaxa/src/lib.rs` (`RecorderFileStore`).

It does not authorize a new representation, garbage collector, public API,
background worker, service, configuration switch, or archive protocol. Remote
archive/checkpoint storage is explicitly **not authoritative** for recorder
promises, accepted values, decisions, or frontier certification. It may carry
validated rebuild material under the separate node checkpoint contract, but it
cannot certify recorder history.

Physical power-cut and ext4/XFS/CSI-specific validation are outside this
contract. In-place repair of a crash-corrupted local filesystem is not claimed.

## Current problem and evidence

`RecorderFileStore` presently has arbitrary old-slot inspection/fetch APIs;
therefore its largest stored slot is not a contiguous, safe-to-discard
frontier. Decision proofs do not necessarily retain command bodies, so a proof
alone cannot always answer a later command fetch. At WAL rotation, checkpoint
creation clones and rewrites active state synchronously.

The deterministic slot-513 regression demonstrates the resulting cliff:
`RECORDER_WAL_HARD_FRAME_LIMIT` is 1,024 frames and the exercised mutation
uses two frames, so slot 513 first crosses that limit. Its synchronous
checkpoint work occurs on the admitted recorder path. See
`RecorderFileStore`, `RECORDER_WAL_HARD_FRAME_LIMIT`, and the checkpoint
rotation tests in `crates/rhiza-quepaxa/src/lib.rs`.

## Definitions

- **Epoch `E` / configuration `C`**: the exact consensus generation and its
  configuration identity/body. A retained fact is never portable to another
  `(E, C)`.
- **Attested frontier `F_a`**: an exclusive boundary: it summarizes contiguous
  decided slots `s < F_a`, with their digest; `F_a` is the next slot. It is a
  local fence, not permission to delete history.
- **Certified GC frontier `F_g`**: an attested prefix plus a future, exact valid
  quorum certificate binding `(E, C, F_g, digest(F_g))`. It prunes `s < F_g`,
  leaves `F_g` as the next retained slot, and is no later than `F_a`.
- **Installed certified frontier `F_c`**: a recorder-local installation of an
  exact certified frontier. For that recorder, `F_c <= F_a`; certification
  never installs beyond its own durable attested fence.
- **Prefix digest chain**: `D_0 = H("rhiza/recorder-prefix/v1" || E || C)` and
  `D_i = H("rhiza/recorder-prefix/v1/step" || D_{i-1} || i || entry_hash_i ||
  decision_proof_hash_i)` for `i` in `1..F`; `D_{F-1}` summarizes slots
  `s < F`. Domain separation, exact slot order, and context binding are
  mandatory.
- **Activation anchor**: the exact Stop/Activate lineage that binds a successor
  configuration to its predecessor and its first valid slot.
- **Retained tail**: all required decision/proof and command-body material for
  `s >= F_g`, plus transition evidence and any body referenced by it.
- **One pending successor**: at most one not-yet-active successor transition;
  it has an exact body/hash/proof and activation status, not a registry.

## Minimal durable conceptual state

The future on-disk encoding is deliberately deferred. Conceptually it stores
exactly:

1. format/version identity (conceptual only until an encoding is approved);
2. active `E` and exact `C` body/identity;
3. activation anchor;
4. local attested `(F_a, D_a)`;
5. certified `(F_g, D_g, quorum certificate)`;
6. optionally, one pending successor `(body, hash, proof, activation status)`.

It stores no per-slot prefix summaries, historical configuration registry, GC
cursor, worker/service state, or second authority. The active WAL and retained
artifacts remain the tail representation.

`DecisionProof` is per-slot evidence; it is not this frontier certificate. The
certificate is an irreducible future protocol proof: an exact-configuration
quorum of durable, monotonic frontier attestations. QuePaxa recorder protocol
owns validation; it binds `E`, `C`, `F`, digest, and quorum, with exact
extension, conflict, and install rules. This authorizes only the model, not a
type, API, encoding, or format change.

## Actions and crash ordering

1. **Record/install above the fence.** A promise, accepted value, command, or
   decision is recorded only in the active `(E, C)` and never overwrites or
   contradicts a certified prefix.
2. **Attest.** Scan a contiguous decided prefix from the prior attestation,
   validate each exact proof/context, and advance `(F_a, D_a)` only as one
   monotonic fact.
3. **Certify.** Assemble and validate an exact quorum certificate for the same
   `(E, C, F_a, D_a)`. A recorder validates exact context, digest, and quorum,
   then durably establishes the candidate `(F_a, D_a)` as its local attested
   fence (or atomically verifies that exact fence) before it installs `F_c`.
   Thus `F_c <= F_a` always holds. It installs only an extension of its existing
   certified fact; base publication and GC are permitted only afterwards.
4. **Publish conceptual base.** Future encoding must first durably write all
   referenced immutable tail/base material, then atomically publish the one
   base record/marker, and only then permit deletion. A crash before publication
   leaves the former base and WAL usable; a crash after publication has every
   reference durable. The byte encoding is deferred.
5. **Activate successor.** Durably establish predecessor Stop/transition
   decision, terminal certificate, and successor material/anchor before active
   configuration publication. Publish the active head and pending clear as one
   logical atomic commit using the existing intent discipline. Crash recovery is
   either old active plus pending/inactive successor, or fully validated
   successor—never active without its anchor or two active configurations.
6. **Recover.** Load the validated local base, then replay the WAL suffix; reject
   malformed, regressing, foreign-context, or incomplete state.
7. **GC.** Delete only slots `s < F_g`, after base publication.

No action obtains authority from archive state. `durability.rs` checkpoint
prepare/install remains a separate node-level consumer of validated snapshots.

## Old-slot and command-body contract

For `slot < F_a`, mutation and proof-install requests fail explicitly; they do
not reopen a locally attested prefix. Old inspection/proof/fetch conceptually
has exactly four outcomes: **Present**; **Absent** (genuinely absent and neither
certified nor pruned); **PrunedUnavailable**; or **CorruptIncomplete** (fail
closed). `Result<Option<_>>` is only pre-change evidence and is inadequate;
the public/wire change needs separate approval. No retry may infer a missing
proof, downgrade `PrunedUnavailable`, or weaken safety. Transition receipts use
their dedicated transition path.

Command bodies have three retention classes:

- bodies referenced by a retained accepted/decided value or transition receipt
  stay available in the retained tail;
- a body whose digest is independently reconstructible may be evicted only when
  the documented fetch contract returns explicit unavailability;
- bodies not referenced by any retained fact may be removed with their certified
  prefix.

The tail-reference rule is simple: no retained proof, accepted value,
transition receipt, pending transition, or activation record may reference a
command/body/artifact that its supported fetch path cannot return or explicitly
classify as unavailable. Shared bodies stay protected until all retained
references are gone. A concurrent reference/GC race is resolved at a pinned
authority generation/commit boundary; this imports neither an archive Reader
lease nor a new subsystem.

## Required invariants

The implementation and model must establish:

1. agreement; valid proof installation; certificate soundness, uniqueness, and
   extension;
2. configuration isolation, a single active configuration, and safe successor
   transition;
3. monotonic fencing and recovery, including no stale base over newer local
   state;
4. old-slot safety and GC safety;
5. tail completeness and no fabricated command/proof answer;
6. no dual authority between local recorder certification and remote archive.

## Smallest bounded model

`formal/recorder_retention_core.pml`, checked with SPIN 6.5.2, is the first
bounded core: three recorders, three slots, one configuration, one
crash/recovery path, and prefix GC. It models record/proof installation,
decision, contiguous attestation, quorum certification, base publication,
crash recovery, old-slot inspection, and GC. Its separate unseeded, seeded,
post-install, and result-witness profiles explore reordered actions and verify
the applicable core invariants above. This is bounded evidence, not a proof of
the complete runtime.

The next model leaf adds five recorders and one Stop-to-Activate successor
transition without widening the runtime design. It must cover independent
old/new quorums, delayed or duplicated proof delivery, conflicting valid-looking
input, crash at each transition publication seam, and fetch after pruning.
Timing and filesystem-specific claims remain outside this model.

## Deterministic runtime contract tests

| Case | Required observation |
| --- | --- |
| gap before a high decided slot | `F_a` does not jump over the gap |
| equivalent/conflicting certificates | equivalent extends idempotently; conflict fails closed |
| crash at each base-publication seam | recover old base+WAL or complete published base; never mixed authority |
| GC with active tail or activation/pending reference | all referenced bodies/artifacts remain protected |
| old mutation/proof install | `slot < F_a` rejects deterministically |
| old inspect/proof/fetch | exact Present/Absent/PrunedUnavailable/CorruptIncomplete mapping; never inferred absence |
| Stop/Activate and pending successor | exactly one successor; no cross-config proof or dual active config |
| activation crash seams | old active plus pending/inactive successor, or anchored validated successor |
| restart/replay | certified/attested facts only advance and tail replays completely |
| concurrent record/attest/certify/GC | no delete before exact certified base publication or pinned-reference boundary |

Existing anchors include `RecorderFileStore`, `DecisionProof`,
`inspect_decision_proof`, command fetch/store methods, and configuration
activation helpers in `crates/rhiza-quepaxa/src/lib.rs`; node snapshot handling
is anchored at `PreparedCheckpointRestore` and install functions in
`crates/rhiza-node/src/durability.rs`.

## Compatibility, operability, and implementation gates

Before encoding, freeze one persisted-format envelope/version and the smallest
supported reader/writer policy. Unsupported versions and mixed states fail
closed; there is no general migration framework. A rolling-upgrade or rollback
promise may be added only with its exact N/N+1 test matrix.

Diagnostics must expose active `(E, C)`, attested/certified frontier and digest
identity, pending successor status, retained-tail bounds, and explicit prune
reason without exposing command bodies. Capacity controls must bound WAL/tail
growth and certification contention without background repair machinery.

Implementation may begin only after: (a) this representation/authority boundary
is reviewed, (b) the bounded model is selected and its actions map to runtime
tests, (c) crash-order tests pass, (d) the public old-slot error contract is
frozen, and (e) a measured benchmark compares normal writes and rotation.

After proof, delete the full-rewrite checkpoint path and any superseded
checkpoint-specific test seam, duplicate old-slot path, or obsolete state. Do
not ship dual retention mechanisms, aliases, compatibility shims, or a second
snapshot service.

## Surface and complexity ledger

| Item | Contract |
| --- | --- |
| Reused primitives | `RecorderFileStore`, WAL replay, `DecisionProof`, command hash/body validation, existing Stop/Activate lineage, atomic publication primitives |
| New surface now | one documentation file only |
| Future irreducible state | one base fact: active context, two frontiers, one optional successor |
| Explicitly not added | per-slot summaries, history registry, GC cursor, worker/service, archive authority, public API/configuration mode |
| Planned deletion/merge | full-state rewrite checkpoint and duplicate retention/old-slot routes after the single base+tail path proves safe |
| Net complexity goal | fewer recovery and retention paths, one authority, one base+WAL-tail recovery path, and a smaller model state space |
