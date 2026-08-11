# Changelog

## Unreleased

- Makes checkpoint coordinator startup consume one manifest/restore pair, so
  a concurrent peer publication cannot be misclassified as checkpoint
  corruption.
- Keeps Recorder RPC `UnknownOutcome` retryable and non-fatal at runtime while
  preserving fatal handling for contradictory durable evidence, and rechecks
  configuration activity before every retried write proposal.
- Reports admitted writes whose response deadline expires as
  `write_outcome_unknown`; retrying the same request ID reconciles the exact
  result instead of treating a possibly committed write as a definite timeout.

## v0.7.0

- Replaces SQLite speculative full-database clones with a clone-free native-WAL
  preparation path and adds a bounded persistent read-connection pool.
- Adds quorum-finalized, content-addressed external SQL effects so atomic
  `CREATE INDEX` and bulk `UPDATE` operations can exceed the 512 KiB consensus
  command limit while remaining bounded to 64 MiB.
- Carries external SQL effects through learner catch-up, checkpoints, archive
  restore, Recorder rehydration, and checkpoint-certified garbage collection.
- Introduces a clean-install storage-generation boundary. Existing pre-v0.7.0
  durable roots and archive namespaces are intentionally not migrated or
  accepted by this release.
- Hardens postcard/TCP Recorder operation, synchronous durability recovery,
  FastPath proof publication, root-anchored durable I/O, and failure evidence.
- Adds an opt-in client ingress-routing tuner with bounded observations and
  fail-safe static routing fallback. QuePaxa proposer preference and hedge
  delay remain static.
- Adds pinned Chaos Mesh qualification tooling and three-peer Rhiza/Hiqlite
  comparison programs. Chaos results are not physical power-loss evidence.

### Release scope

`v0.7.0` publishes GitHub source, SQL/Graph/KV CLI archives, and immutable
multi-architecture GHCR images. Rust crate versions remain independently
versioned and are not published by this release.

## v0.6.1

- Removes unreachable match arm in KV batch handler that caused clippy failure
  under `-D warnings` in the KV profile CI.

## v0.6.0

- Promotes Graph and KV profiles to production-ready status.
- Adds graph mutation endpoints (`/v1/graph/documents/put`,
  `/v1/graph/documents/delete`) and corresponding CLI commands.
- Adds KV batch API endpoint (`/v1/kv/batch`) for atomic multi-key operations.
- Adds profile-scoped Kubernetes client Services (`rhiza-graph-client`,
  `rhiza-kv-client`).
- Replaces `expect()` panics in KV codec with proper error propagation.
- Updates documentation to reflect production readiness of all profiles.

## v0.5.1

- Publishes profile-isolated GHCR images for both Linux amd64 and arm64,
  plus SQL/Graph/KV CLI binaries for Linux, macOS, and Windows on x64 and arm64.

## v0.5.0

- Restores KV/redb as an isolated server profile with authenticated put,
  delete, get, and scan HTTP APIs, typed `rhiza-client` SDK methods, CLI
  commands, checkpoint recovery, Kubernetes rendering, and CI coverage.
- Prepares the crates.io dependency chain: `rhiza-kv` 0.1.0,
  `rhiza-graph` 0.2.0, `rhiza-node` 0.4.0, `rhizadb` 0.4.0, and
  `rhiza-client` 0.1.0.
- Adds protected crates.io publication and automatic profile-specific Linux
  CLI assets and GHCR images for published GitHub releases.

## v0.4.0

- Restores Graph/LadybugDB as a supported opt-in execution profile while SQL
  remains the default and KV remains excluded.
- Adds isolated Graph features through the node, client, embedded facade, and
  CLI, including Graph checkpoint and recovery support.
- Adds Graph CI dependency isolation, container builds, and beta Kubernetes
  rendering and administration helpers.
- Consolidates Ladybug metadata, request-receipt, and document-existence
  lookups. A fixed-binary raw materializer benchmark measured +1.7% at batch 1,
  +37.1% at batch 8, and +87.1% at batch 32.
- Avoids a second JSON serialization on successful Graph HTTP queries.

### Release scope

`v0.4.0` is a GitHub source release. It does not publish crates.io packages or
OCI images. Existing crate versions remain independently versioned; the latest
published `rhizadb` crate remains the SQL-only v0.3.0 artifact.

Graph Kubernetes support is beta until a multi-node cluster smoke result is
published. Graph runtime, embedded API, checkpoint recovery, isolated release
build, and container build paths are supported.
