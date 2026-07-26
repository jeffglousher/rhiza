# Changelog

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
