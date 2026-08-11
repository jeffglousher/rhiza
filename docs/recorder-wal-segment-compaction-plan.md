# Recorder WAL segment and compaction plan

Status: accepted design direction; not implemented by the benchmark-standard
change. This plan addresses the reproducible 1,024-frame rotation failure
without weakening the five-second RPC deadline or the `UnknownOutcome` safety
classification.

## Evidence and decision

The SQL runtime benchmark with three local file recorders, 200 warmup writes,
2,000 measured writes, concurrency 1, and batch size 1 fails when synchronous
rotation materializes roughly 1,024 slot and command cache files under the
recorder serialization lock. A prototype that materialized the preceding
WAL-durable overlay before every append completed the run but reduced throughput
from about 55.35 to 18.18 operations/second and increased p50 from about 17.3 to
53.6 milliseconds. It was discarded.

Use immutable WAL segments and asynchronous stable-cache compaction. Do not put
a synchronous aggregate checkpoint in the RPC path: encoding, writing, syncing,
renaming, directory syncing, and WAL reclamation still have no hardware-
independent five-second upper bound.

## Required invariants

- The normal foreground path performs one WAL sync per accepted record frame.
  Pre-stored command RPCs retain their separate durability contract.
- Rotation performs no stable-cache write, head publication, file creation,
  rename, directory sync, or WAL truncation.
- Sequence numbers and the digest chain remain monotonic across segments.
- A frame write or sync failure poisons the recorder and retains
  `UnknownOutcome`; admission failures before a write are definite rejections.
- Stable cache writes become authoritative only when a new recorded-head
  manifest is durably published.
- Segments are deleted only after that publication.
- Startup replay performs no opportunistic checkpoint. It may apply only
  deterministic, verified recovery actions: repair a versioned
  configuration/head intent, truncate a verified torn active suffix, remove
  orphaned temporary files, and prepare durable reserves.

## Durable format

Introduce an explicit clean-install format break:

```text
recorder-root/
  recorded-head.rec
  configuration.rec
  stable-index-<compaction-generation>.rec
  stable/
    command/<content-digest>.cmd
    slot/<content-digest>.rec
  wal/
    segment-00000000000000000001.qwal
    segment-00000000000000000002.qwal
    segment-00000000000000000003.qwal
```

Recorded-head v4 separates compaction generation from WAL segment identity and
contains at least the compacted-through segment, sequence and tail digest, the
configuration identity, and the digest of an immutable stable-cache index.
That index contains the complete stable authority through the cut as sorted
`(slot, immutable-object-digest)` and `(command-hash,
immutable-object-digest)` entries plus counts and a checksum; each compaction
carries the prior published index forward and applies the sealed segment's
final updates. Stable objects are content-addressed, immutable, created
exclusively, and never replaced in place. Every stable lookup validates the
selected object against the published index; the WAL segment is not deleted
until all referenced objects, the index, and the head are durable.
Enumeration derives only from that complete published index, never from an
untrusted directory listing. The head binds the exact index generation and
digest.

Segment headers bind recorder identity and segment/predecessor IDs. The first
frame in each segment binds the previous segment's final digest. Filenames are
derived from numeric IDs; the manifest never stores arbitrary paths.

Active and reserve identity does not require a head write during rotation.
Recovery requires contiguous non-empty segments after the compacted cut,
followed only by header-complete empty segments. The highest non-empty segment
is active when it is below its limit; when full, the lowest following empty
segment becomes active at the next admission. Higher empty segments are
reserves. When all segments after the cut are empty, the lowest valid empty
segment is active and the rest are reserves. Gaps, a non-empty segment after an
empty one, duplicate IDs, or invalid headers fail closed. A crash after an
in-memory switch but before the first frame therefore safely reconstructs the
empty segment as unused.

A structural configuration transition is the exception to size-derived active
selection. Recorded-head v4 contains an ordered transition-anchor chain. Each
anchor binds predecessor and successor configuration identities, the durably
sealed segment ID, its final sequence and digest, and the transition proof.
Recovery treats every anchor-named segment as sealed even below its size limit
and changes configuration identity only at the exact anchored sequence.

Existing non-empty v3 roots fail with an explicit incompatible-format error.
No partial migration may infer authority from a mixture of old cache files and
new segments.

## Foreground protocol

Opening a recorder prepares the active segment plus at least two empty,
fully-written, file-synced, renamed, and directory-synced reserve segments.
Before any mutation write, admission validates identity and the encoded frame,
checks arithmetic and all backlog/disk quotas, and verifies that a durable
reserve exists if rotation is needed.

At rotation the recorder only moves the old active metadata to the sealed queue
and swaps in an already-durable reserve. It then writes and syncs the new frame
once. Sequence, tail digest, counters, and sequence-tagged slot/command overlays
change only after that sync succeeds.

The background maintenance worker replenishes consumed reserves independently
of compaction. It writes and syncs the header, renames and directory-syncs the
segment, then registers it in memory. Failure to replenish is observable and
eventually causes a definite pre-admission rejection when the durable reserve
floor is reached; foreground rotation never creates a segment.

The hard admission limits cover uncompacted segments, frames, WAL bytes, and
total managed physical bytes, including undeleted compacted segments and
temporary files. Exceeding a limit rejects before `write_all`; it must not
produce an unknown outcome.

## Compaction protocol

One compactor per recorder selects and pins the oldest sealed segment under a
short lock, then performs all decoding and stable-file I/O outside the append
lock. A host-wide semaphore bounds simultaneous recorder compaction I/O.
Before creating any stable object or candidate index it also installs a writer
candidate pin. GC reachability and physical-byte accounting include every
object and index owned by that pin. The pin is released only after successful
head CAS transfers reachability to the published view, or after an aborted
candidate is safely cleaned up.

The durable order is:

1. Validate the complete segment from the manifest sequence/digest anchor.
2. Materialize deduplicated commands and final slot states as immutable,
   content-addressed objects without changing the current read view.
3. Make every new stable object durable and write an immutable, checksummed stable
   index that commits the exact file contents.
4. Atomically publish and directory-sync recorded-head v4 through the segment,
   referencing that stable-index digest and the unchanged configuration
   identity.
5. Under a short lock, remove only overlay entries whose sequence is at or
   below the published cut; preserve newer entries.
6. Unpin and delete covered segments, then sync the WAL directory.

If head publication is ambiguous, the compactor rereads it. When authority
cannot be established, compaction stops and foreground writes continue only to
the pre-admission backlog limit. A transient compaction error is not itself a
fatal acknowledged-data error; verified corruption is fail-closed.

Publication is a compare-and-swap on the complete prior head identity:
configuration identity, compaction generation, stable-index digest, and
transition-anchor-chain digest. The compactor reacquires the short publication
lock and verifies that identity before replacing the head. A mismatch discards
or rebases the candidate; it never deletes segments or removes overlays. Its
unreferenced immutable objects remain invisible and are safe for later GC. This
prevents an out-of-lock compactor from overwriting a concurrent transition or a
newer compaction publication.

The recorder directory lock remains held until the compactor has stopped.
Graceful shutdown need not finish compaction because retained segments are the
redo authority.

Compaction never performs a structural configuration transition. A transition
seals the current segment and uses a versioned form of the existing
`configuration-head.intent` protocol so recovery can deterministically finish
or reject the configuration/head pair. Recorded-head v4 binds the authoritative
configuration digest; `configuration.rec` must match it before replay. A
compactor may publish only within that configuration identity.

Successor publication does not discard uncompacted predecessor history. The
versioned intent atomically adds the transition anchor to recorded-head, and
predecessor segments remain pinned until compaction advances the stable index
through the anchor and its proof. Replay begins at the compacted cut, validates
each retained predecessor segment under its recorded configuration, verifies
the anchored transition, and then validates successor frames under the new
identity. Once the compaction cut passes an anchor, the full stable index and
head retain the transition proof needed to delete the predecessor segments.

## Recovery rules

Startup validates the manifest and scans deterministic segment names. It rejects
ID gaps, forks, header/filename or identity mismatches, sequence gaps or
duplicates, non-empty-after-empty ordering, cross-segment digest mismatches, and
corruption in any sealed segment. Only the highest non-empty active candidate
may have a torn final suffix. Replay starts at the manifest sequence/digest,
rebuilds pending overlays and counters, then prepares the required durable
reserves before write admission.

Startup validates the head-referenced stable index before treating cache files
as authority. Each cache file is matched to its indexed digest when loaded;
missing, stale, or substituted cache content fails closed. Orphaned newer index
files are ignored until deterministic cleanup because recorded-head is the only
publication point.

The in-memory read view contains one immutable head/index identity. A successful
head CAS atomically swaps that view; concurrent readers using the predecessor
view continue to reference predecessor immutable objects. Garbage collection
removes an object only after no published or reader-pinned index references it.

Every read and enumeration path uses pending overlay state before stable cache
state. This precedence must be proven for slots, commands, ranges, recorded-head
and configuration views, not assumed from single-key tests.

## Release gates

The format and recovery implementation is not production-enabled until all of
these pass:

- Deterministic failpoints before/after frame write and sync, reserve publish,
  active switch and the first frame in a reserve, every compaction publication
  boundary, overlay removal, and segment deletion.
- Corruption cases for sealed segments, active torn suffixes, gaps, forks,
  non-empty-after-empty ordering, identity mismatch, digest mismatch, stable
  index/cache mismatch, and manifest mismatch.
- A sub-limit segment sealed by configuration transition remains sealed after
  crashes before/after intent publication, successor-head publication, and the
  first successor frame; replay crosses the configuration anchor exactly once.
- Concurrent update of a slot while its older segment is compacted.
- Concurrent configuration/head or compaction publication after index snapshot
  forces CAS failure/rebase without segment deletion or overlay removal.
- Reads and reopen under the old head remain valid while candidate immutable
  objects are written and after a forced CAS failure; unreferenced candidates
  remain invisible and are safely collectable.
- Concurrent GC cannot remove an in-flight candidate index or object protected
  by its writer pin; publication either installs every referenced object or
  rebases without head, segment, or overlay mutation.
- Backlog rejection leaves WAL lengths, sequence, digest, overlays, stable
  files, and manifest unchanged; compaction later restores admission.
- A 2,049-operation inline run and a pre-stored-command run each observe exactly
  two rotations, zero errors, a contiguous digest chain, one foreground WAL
  sync per accepted operation, and no foreground stable/head/directory sync.
- Reopen verifies every slot and every command, before and after forced
  compaction through sequence 2,048.
- The three-recorder SQL runtime workload above completes with zero unknown
  outcomes and zero fatal recorder states, using the unchanged five-second
  deadline and rotation thresholds.
- On the pinned reference host, the two-rotation run achieves at least 90% of
  the sub-threshold control throughput and p50 no more than 125% of control.
  Absolute performance is not a portable CI gate.
- A soak proves sealed-segment backlog converges instead of merely postponing
  failure.

Implementation stages are: v4 segment decoder/recovery behind a disabled path;
durable reserves and bounded foreground rotation; asynchronous compactor and
lifetime management; then the runtime, recovery, and soak gates. Foreground
rotation and compaction ship together—bounded rotation without compaction only
delays eventual write rejection.
