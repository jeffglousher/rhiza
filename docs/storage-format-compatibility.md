# Persisted-format compatibility baseline

Status: inventory and design boundary, not an implemented mixed-version
promise. Current readers accept their exact current envelope; current writers
write that envelope. Adjacent N/N+1 storage compatibility is **not
implemented**.

This is the single canonical inventory of Rhiza durable artifacts. "Authority"
means the artifact is needed to establish a valid local or remote state;
"reconstructible" means a discarded node-local copy may be rebuilt only from
validated remote/quorum evidence and ordered replay. It never permits trusting
or repairing an arbitrary corrupt local directory.

## Canonical persisted-artifact matrix

| Artifact | Owner / path or key | Authority or reconstruction | Envelope / current reader → writer | Validation, failure, and rebuild |
| --- | --- | --- | --- | --- |
| Qlog segments | `rhiza-log`; `<qlog>/{start}-{end}.qlog`, `{start}-open.qlog` | Ordered-log authority; a disposable local copy is rebuilt from a validated checkpoint plus suffix | `QLOG` v1 frames/footers; exact → v1 | Bounded canonical frames, checksums, hash chain, and anchors; reject mismatch before replay |
| Qlog compaction controls | `rhiza-log`; `.truncate-intent`, `.compact-intent`, `recovery.anchor` | Crash/compaction authority; anchor is rebuilt only by validated checkpoint/compaction | `QTRN` v1, `QCMP` v1, `QANC` v4 carrying core `RecoveryAnchor` v2; exact → current | No-follow/reparse-safe regular-file reads cap each intent at `CONTROL_INTENT_MAX_BYTES` = 8388608 bytes (8 MiB); intent fsync fences replacement; malformed, stale, or inconsistent anchor fails closed |
| Replicated command/effect payloads | `rhiza-core`; `qlog/recorder/checkpoint payload` | Consensus authority; recovered with its containing log/checkpoint | Canonical command/config envelopes and `QEFX\0\x01`; exact → current | Canonical bounded decode and matching proof/configuration are required |
| Recorder generation and lock | `rhiza-quepaxa`; `.rhiza-storage-generation`, `.recorder.lock` | Generation marker is local layout authority; lock is operational exclusion; fresh recorder may be reconstructed only through validated recovery | Literal `rhiza:recorder:storage-generation:clean-v1\n`; no lock envelope | Anchored bounded regular-file read; missing/mismatched marker and lock failure reject open/install |
| Recorder decision state | `rhiza-quepaxa`; `recorder.wal`, `recorded-head.rec`, `slot-{slot}.rec` | Recorder/proof authority; reconstruct only from validated recorder/checkpoint recovery | `QWAL` v1, `QRHD` v3, `QREC` v4; exact → current | Checksums, generation/sequence, canonical proofs, and torn-tail rules are enforced; conflict fails closed |
| Recorder configuration/commands | `rhiza-quepaxa`; `configuration.rec`, `configuration.intent`, `configuration-head.intent`, `command-{hash}.cmd` | Membership, transition, and proof-resolution authority | `QCON` v3, `QINT` v1, `QCHI` v1, `QCMD` v1; exact → current | Identity, ordered transition, and content-hash checks; recovery replays only a coherent set |
| Recorder effects and GC fence | `rhiza-quepaxa`; `effect-bundle-*`, effect chunks, `.effect-bundle-gc-anchor.rec` | Finalized effect bytes and GC anchor are authoritative while referenced; staged chunk ACKs are process-local | `QEFX` bundle/chunks and `QEGC` v1; exact → current | Chunk/object count, digest, aggregate-size, and publication-receipt checks; restage every chunk after restart; unsafe deletion fails closed |
| SQL materialization and control | `rhiza-sql`; `db.sqlite`, SQLite sidecars, `.rhiza-control.sqlite` | Local materialization/control authority, reconstructible from snapshot plus replay | SQLite plus `QCTL` schema v6, `QWAL` v3 effects, `QSNP` v4 snapshot; exact → current | Page state, canonical effects, receipts, schema and executor fingerprint are verified; install snapshot rather than auto-migrate |
| KV materialization | `rhiza-kv`; `<data>/kv/data.redb` | Reconstructible materialized state | redb tables plus `RHKV` v1, `RHKB` v3, `RHKR` v1, `RHKS` v1; exact → current | Schema/fingerprint, bounded decode, state root, and replay continuity are required |
| Graph materialization | `rhiza-graph`; `<data>/ladybug/graph.lbug` | Reconstructible materialized state | Ladybug storage plus `RHGC` v1, `RHGB` v2, `RHGR` v1, `RHGS` v1; exact → current | Storage version/schema/materializer fingerprint and snapshot root are checked before use |
| Archive history | `rhiza-archive`; `rhiza/{cluster}/archive/manifest.json`, segments, profile snapshots | Remote history/rebuild authority | Archive v1; contained QLOG v1 and profile snapshot envelope; exact → current | Immutable object metadata, canonical manifest, hashes, profile/fingerprint, and CAS publication are checked |
| Checkpoint generation | `rhiza-archive`; `rhiza/{cluster}/checkpoints/epoch-{e}/config-{c}-digest-{digest}/generation-{g}/manifest.json`, `segments/`, `snapshots/` | Primary remote node-rebuild authority | Checkpoint v3; QLOG v1, SQL `QSNP` v4, Graph/KV snapshot v1; exact → current | Reader lease pins one manifest; object/segment/decoded limits, identity, root, hashes, configuration digest, and completeness fail closed before install |
| Checkpoint publication receipts | `rhiza-archive`; generation `receipts/{holder-hash}/{manifest-digest}.json` | Immutable publication/GC authority, not replaceable by local inference | Bounded canonical JSON receipt; exact → current | Receipt binds checkpoint identity and visible manifest/object version; differing same-slot evidence conflicts |
| Archive control and leases | `rhiza-archive`; `gc/control.json`, `plans/{hash}.json`, `reports/{hash}.json`, lease records | Retention/reader/publisher fencing authority; expired operational leases are reacquired, plans regenerated from current evidence | GC v1 JSON; exact → current | CAS, lease deadline, holder and checked plan/report contents fence deletion; mismatch fails closed |
| Restore/install state | `rhiza-node`; `.node.lock`, `.rhiza-restore.json`, `.rhiza-checkpoint-install.json`, `.rhiza-checkpoint-identity.json`, `.restore-stage-*`, `.restore-marker-tmp-*` | Local atomic-install and monotonicity authority; discard/rebuild only through prepare → validate → fenced install | Bounded JSON; restore intent v2, install receipt v2, local marker v2; exact → current | Cross-process lock and configuration-digest-bound path/profile/tip identity prevent stale, cross-membership, or partial activation |
| Restore QEFX and recovery ownership | `rhiza-node`; `consensus/qefx-restore/`, `consensus/pending-qefx-gc.json`, `.rhiza-recovery-owner.json` | Prepared effects/GC maintenance and repair-owner fences; reconstruct from matching validated checkpoint receipt only | Canonical effect bytes and bounded JSON; exact → current | QEFX suffix/reference bijection, aggregate limits, receipt equality, digest-bound owner identity, and atomic publish/remove checks fail closed |
| Successor/prestage activation | `rhiza-node`; `.successor-restore.{lock,intent,complete}`, `.successor-prestage.{lock,intent,ready,published,finalized}` | Membership activation and recovery fencing authority | Bounded JSON; successor restore receipt v2, prestage identity v2; exact → current | Exact predecessor/target membership digests, checkpoint identity, proof, and state transition are required; incomplete transitions are not activated |
| Completion markers | `rhiza-node`; `<data-dir>/<portable-marker-name>` caller-supplied validated portable relative name | Local install fence owned by its receipt | Marker name is portable ASCII and receipt-bound; no separate envelope | Dot/device/ADS/trailing-name rejection, regular-file checks and receipt hash bind marker |
| Admin operation ledger | `rhiza-node`; `<data>/admin-operations-v2.json` | Bounded local replay authority for async admin results, not consensus or rebuild authority; discarded with a rebuilt node | Strict version 2 `OperationLedger` JSON; `BTreeMap`, SHA-256 request fingerprint; `ADMIN_OPERATION_LEDGER_MAX_BYTES` = 1048576, `ADMIN_OPERATION_LEDGER_MAX_RECORDS` = 1024, `ADMIN_OPERATION_RESULT_MAX_BYTES` = 65536, `ADMIN_OPERATION_RETENTION_SECS` = 2592000; exact → current | Sequence, time, ID, status, digest, regular-file and size validation fail closed; expiry then oldest-first eviction protects the new ID; failed durable commit disables admin mutations as `503 unavailable` |

Source anchors: qlog formats and intents are in
[`rhiza-log`](../crates/rhiza-log/src/lib.rs); recorder envelopes and local
paths are in [`rhiza-quepaxa`](../crates/rhiza-quepaxa/src/lib.rs); archive
versions, leases, and receipts are in
[`rhiza-archive`](../crates/rhiza-archive/src/lib.rs); restore/install artifacts
are in [`rhiza-node/durability`](../crates/rhiza-node/src/durability.rs); and
profile payloads are in the `rhiza-{sql,kv,graph}` crates.

## Current boundary

There is no in-place migrator, compatibility alias, or supported mixed-binary
writer protocol today. An unknown, newer, mismatched, incomplete, or
noncanonical artifact must be rejected; a valid older remote checkpoint may be
chosen only when its own validation succeeds and the ordered suffix can be
verified. This inventory does not claim physical power-cut, ext4/XFS/CSI, or
in-place corrupt-volume repair validation.

## Design-only adjacent upgrade policy

Before publication, the narrow intended promise is adjacent N/N+1 only:

1. During an N/N+1 mixed phase, N+1 reads N and writes the N-readable form.
2. Only after every node has cut over may any writer emit a new durable form.
3. Once that new form exists, downgrade to N is unsupported and must fail
   closed.
4. Each changed row must name accepted reader versions and writer selection;
   no generic migration framework or compatibility aliases are introduced.

Required evidence before that promise can be made: old fixtures opened by N+1;
an N writer accepted by N+1; rolling N/N+1 with no premature new write;
unsupported-version failure; post-cutover downgrade failure; and the same
matrix exercised through checkpoint restore, qlog/recorder recovery, GC, and
membership activation.
