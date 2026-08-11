# Taldra-maintained Rhiza fork

This repository is a maintained fork of [mrchypark/rhiza](https://github.com/mrchypark/rhiza)
for [Taldra](https://github.com/jeffglousher/taldra).

Rhiza / `rhiza-quepaxa` is treated as **experimental**. Taldra does **not**
depend on crates.io `rhiza-quepaxa`: that registry package also claims version
`0.3.0` but ships an older public API (`Consensus::propose(&self, Command)`
without `RecorderRpcContext`). The audited git surface requires caller-owned
deadlines and cancellation via `RecorderRpcContext`.

## Policy

1. **Source of truth for Taldra:** this fork (`jeffglousher/rhiza`), not crates.io.
2. **Pin exact revisions** in Taldra `Cargo.toml` / `Cargo.lock`; do not float
   on a moving branch tip without a deliberate bump.
3. **Sync from upstream** periodically with merge or rebase onto
   `mrchypark/rhiza` `main`, then re-run Rhiza and Taldra gates before bumping
   the Taldra pin.
4. **Taldra-required changes** (actor-owned drive seams, bounds, no product-type
   leakage helpers) land as commits on this fork first, then Taldra retargets the
   pin. Prefer minimal, reviewable patches over silent rewrites.
5. **Do not** publish a competing crates.io crate from this fork unless the
   owner Accepts a versioning/identity plan.

## Current Taldra pin baseline

At fork adoption, Taldra tracks tip of this fork after the `archive-gc`
optionalization patch (see below). Exact revision is pinned in Taldra
`crates/taldra-consensus/Cargo.toml`.

See Taldra ADR-0005 / ADR-0015 and `deny.toml` `allow-git` for
`https://github.com/jeffglousher/rhiza`.

## Fork patches (Taldra)

- `archive-gc` feature: `rhiza-quepaxa` no longer hard-depends on
  `rhiza-archive` / `object_store` / `aws-lc-sys`. Enable `archive-gc` only when
  checkpoint GC APIs are required. Taldra's dual-engine pin uses
  `default-features = false` and does not enable `archive-gc`.

- Windows / non-unix64 `anchored_fs` stub: import `crate::{Error, Result}` so
  the unsupported platform path type-checks (upstream tip omitted the import).


## Next fork patch (planned)

**Caller-owned / actor-owned drive seam (ADR-0005 exit condition 1):**

- Keep `RecorderRpc` as the transport-independent recorder contract.
- Add a caller-driven propose/drive path that does **not** spawn
  `RecordWorker` / `ControlWorker` / read-fence OS threads per cluster.
- Dispatch record / install / inspect RPCs on the calling thread (or through a
  Taldra-owned shared bounded I/O pool later), returning
  `UnknownOutcome` on deadline/cancel after mutation may have started.
- Preserve file-backed recorder durability and proof-install semantics.
- `ThreeNodeConsensus` worker runtime remains available for Rhiza-native
  tests; Taldra lab will migrate off it after the seam lands and the pin bumps.

Until that patch merges, Taldra comparative labs may use `ThreeNodeConsensus`
only as a temporary harness (documented in ADR-0015).
