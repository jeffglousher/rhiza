# Public API and wire compatibility baseline

Status: first-publication boundary design. It distinguishes the APIs intended
for consumers from implementation visibility; it does **not** promise that
every Rust `pub` item is stable. The durable-artifact inventory lives only in
[the persisted-format baseline](storage-format-compatibility.md).

## Intended consumer boundary

| Surface | Intended use | Compatibility status |
| --- | --- | --- |
| `rhizadb` facade | Embedded SQL/Graph owner (`Rhiza`, `EmbeddedConfig`, lifecycle, documented traits) | Primary embedded consumer surface; exact symbols still need a pre-publication API index |
| `rhiza-client` | Typed authenticated remote HTTP client and its documented request/result types | Primary remote Rust consumer surface; coordinated wire changes required |
| `rhiza-quepaxa` | Transport-independent consensus, recorder RPC, proofs, and documented durable recorder integration | Advanced component API; transport and archive policy remain outside it |
| `rhiza-archive` / `rhiza-node` | Checkpoint/recovery/runtime owners for integrators that need them | Advanced component APIs; use documented constructors and lifecycle operations, not incidental exports |
| Test hooks, benchmark fixtures, testkit, and internal support modules | Repository verification only | Non-contract; may change or disappear before publication |

No compatibility aliases will be added for unreleased names. A separate
pre-publication cleanup leaf may remove duplicate/legacy exports rather than
preserving them.

## Current wire boundary

The current implementation accepts exact protocol values only; adjacent or
mixed N/N+1 wire compatibility is **not implemented**.

| Protocol | Current boundary | Failure behavior |
| --- | --- | --- |
| Service HTTP | `x-rhiza-version: 1`; documented `/v1/...` node routes | Missing or unequal header is rejected before the operation |
| Recorder HTTP/TCP | recorder wire version `5`; recorder protocol header value `5` | Mismatched command/record/header is a decode/protocol failure |
| Recorder postcard RPC | sealed postcard envelope version `6` | Header/sequence/body mismatch is rejected; a sent mutation retains unknown-outcome fencing |
| Learner tail | `x-rhiza-tail-version: 1` | Mismatch is rejected before tail processing |

The source of truth is [`rhiza-node`](../crates/rhiza-node/src/lib.rs) and
[`recorder_tcp`](../crates/rhiza-node/src/recorder_tcp.rs); `rhiza-client`
uses the same node wire vocabulary. A wire value is neither a consensus latency
target nor evidence of storage compatibility.

[`scripts/e2e-mixed-wire-v071.sh`](../scripts/e2e-mixed-wire-v071.sh) is a
manual gate with exactly SQL/tcp-postcard, SQL/tcp-postcard-rpc,
KV/tcp-postcard, and Graph/tcp-postcard cells. It proves bidirectional HTTP v1
and recorder TCP v5 for those cells, plus postcard RPC v6 only for SQL, between
v0.7.1 and the current committed HEAD. Each cell performs a coordinated clean
restart: the current node 1 binary reopens its v0.7.1-written node 1 root, and
the v0.7.1 node 3 binary reopens its current-written node 3 root; ownership is
unchanged. It is empirical evidence only
for those two commits—not broad format stability, SemVer, rolling upgrade,
crash/checkpoint recovery, learner, TLS, membership, partition, KV/Graph
postcard-RPC, or PR #67 ambiguous-outcome compatibility.

## Design-only adjacent release policy

The smallest future commitment is N/N+1, and only after the persisted-format
matrix is completed:

1. N+1 must read N and emit N-readable wire and durable forms while N remains.
2. All nodes must complete cutover before an N+1-only durable form is written.
3. A downgrade after that write is unsupported and must fail closed.
4. Unsupported mixes fail closed; there is no compatibility mode, alias layer,
   or general migration framework.

Required proof: documented API inventory/semver review; N and N+1 client/node
wire cases; rolling-upgrade and rollback cases; exact-retry/unknown-outcome
coverage; persisted fixture readers/writers; and checkpoint/recovery,
membership, partition, and restart coverage. Until then, package users must
pin one exact compatible release set.
