# rhiza

`rhiza` is a distributed database family ordered by QuePaxa. The agreed qlog
plus an official snapshot is authoritative; a local materialized database file
is not.

## Product Status

The source workspace supports isolated SQL, Graph, and KV runtime profiles.
SQL remains the default; Graph uses LadybugDB and KV uses redb.

The current crates.io product, `rhizadb` v0.3.0, is SQL-only and uses SQLite.
Its registry dependency closure is `rhiza-core`, `rhiza-obj-store`,
`rhiza-log`, `rhiza-quepaxa`, `rhiza-archive`, `rhiza-sql`, `rhiza-node`, and
`rhizadb`. That historical artifact remains SQL-only. The next registry release
adds the Graph dependency chain and the separately published `rhiza-client`
Rust SDK with SQL, Graph, and KV HTTP features; KV remains server/SDK-only and
is not exposed by the embedded `rhizadb` facade.

## Workspace Components

The Kubernetes-independent Rust workspace currently contains:

- `rhizadb`: embedded SQL/Graph Rust facade and lifecycle owner; the published
  v0.3.0 registry product remains SQL-only.
- `rhiza-core`: log, configuration, command, and snapshot domain types.
- `rhiza-quepaxa`: recorder RPC, durable recorder state, and consensus.
- `rhiza-log`: local binary qlog and compaction anchors.
- `rhiza-obj-store`: `object_store` adapters for S3, GCS, Azure, and tests.
- `rhiza-sql`: the `rhiza sql` SQLite materialized-state boundary.
- `rhiza-graph`: supported opt-in LadybugDB state boundary.
- `rhiza-kv`: supported redb state boundary for the KV server profile.
- `rhiza-archive`: checkpoint V2, object metadata, and GC plans.
- `rhiza-node`: runtime, HTTP RPC, recovery, and authenticated live admin HTTP.
- `rhiza-client`: typed Rust SDK for the authenticated remote HTTP API.
- `rhiza-cli`: workspace administration commands.
- `rhiza-testkit`: internal, non-publishable integration-test support.

The protected registry workflow publishes the engine dependency crates before
`rhiza-node`, `rhizadb`, and `rhiza-client`. `rhiza-cli`, `rhiza-testkit`, and
`examples/basic-app-server` are not crates.io products; the CLI is distributed
as profile-specific GitHub Release binaries and OCI images.

## Runtime Profiles and Deployment

SQL remains the default. Build an isolated SQL, Graph, or KV image and set the
matching profile before serving, checkpointing, recovery, GC, or membership
work:

```bash
export RHIZA_EXECUTION_PROFILE=sql
export RHIZA_CLUSTER_ID=cluster-a
docker build --build-arg RHIZA_PROFILE=sql -t rhiza-sql:dev .

export RHIZA_EXECUTION_PROFILE=graph
docker build --build-arg RHIZA_PROFILE=graph -t rhiza-graph:dev .

export RHIZA_EXECUTION_PROFILE=kv
docker build --build-arg RHIZA_PROFILE=kv -t rhiza-kv:dev .
```

The production stage is a digest-pinned Debian 13 distroless `cc` image. It
runs `/usr/local/bin/rhiza` as UID/GID 65532 and intentionally contains no
shell or package manager; mount runtime data and configuration with permissions
that allow this non-root identity to read or write them as required.

Image publication and registry tags are separate from the crates.io release;
see [RELEASING.md](RELEASING.md) for the registry procedure.

## Embedded Rust API

The published `rhizadb` v0.3.0 crate exposes an SQL-only embedded owner. The
v0.4.0 crate adds an opt-in `graph` feature with Graph commands, queries,
checkpoint recovery, and embedded lifecycle support. KV remains outside the
embedded facade and is consumed through `rhiza-client` or the HTTP API.

`Rhiza` owns the node runtime and background workers; cloneable `RhizaHandle`
values are weak handles that stop working after owner shutdown. Keep the owner
alive while serving requests, drain the server first during planned shutdown,
then call `shutdown().await` so durability and worker errors are reported.
Dropping the owner only signals shutdown and cannot report those errors.

### 5-minute consumer app

`examples/basic-app-server` is a separate Cargo package that depends only on
the public `rhizadb` API. Start its local HTTP server with an explicit loopback
address and data directory:

```bash
RHIZA_BIND_ADDR=127.0.0.1:3000 \
RHIZA_DATA_DIR=./rhiza-data \
cargo run -p rhiza-basic-app-server
```

In another terminal:

```bash
curl http://127.0.0.1:3000/ready
curl -X PUT http://127.0.0.1:3000/items/greeting \
  -H 'content-type: application/json' \
  -d '{"request_id":"put-greeting-1","value":"hello"}'
curl http://127.0.0.1:3000/items/greeting
```

Stop the process, run the same command again with the same `RHIZA_DATA_DIR`,
and both the value and an exact replay of the PUT survive. Reusing a
`request_id` with a different key or value returns HTTP 409 with Rhiza's
original classification, for example
`{"error":"request_conflict","retryable":false,...}`. HTTP status is selected
from the classification category; the body preserves its stable code and
retry guidance.

This package intentionally rejects non-loopback `RHIZA_BIND_ADDR` values.
Its three file-backed recorders share one process and data directory, so it is
for local development and consumer integration—not a highly available or
remote-facing deployment.

An embedding application chooses the ownership model explicitly. For
local-only writes, `local_file_backed` creates a fixed three-recorder
configuration below one root directory and opens no peer listener or membership
workflow. All recorders share the process and failure domain, so this mode is
durable but not highly available:

```rust,no_run
use rhizadb::{EmbeddedConfig, ExecutionProfile, Rhiza, ReadConsistency};

async fn example() -> Result<(), rhizadb::Error> {
let config = EmbeddedConfig::local_file_backed(
    "cluster-a",
    "./data",
    ExecutionProfile::Sqlite,
)?;
let owner = Rhiza::open(config).await?;
let db = owner.handle();

db.put("request-1", "key", "value").await?;
let value = db.read("key", ReadConsistency::Local).await?;
assert_eq!(value.value.as_deref(), Some("value"));

owner.shutdown().await?;
Ok(())
}
```

`EmbeddedConfig::new` is an advanced extension point for custom or remote
recorder and log transports. It accepts `RecorderRpc` and `LogPeer` trait
objects, which `rhizadb` re-exports as the narrow extension boundary. Implementing
those traits or using the broader transport vocabulary still requires direct
dependencies on `rhiza-quepaxa` and `rhiza-node`. Normal local consumers should
use `local_file_backed`.

For Hiqlite-style clustered ownership, the embedding application binds the
Recorder and service listeners, builds `HaServeConfig`, and starts either a
normal `HaNode` or a live `HaSuccessorNode`. These public owners manage
preparation, both ingress servers, recovery, checkpoint coordination,
readiness, background workers, and ordered durability-aware shutdown. The
application supplies transport clients and process shutdown; the CLI is only
one adapter over this API.

```rust,no_run
use std::{future::Future, sync::Arc};

use rhizadb::{
    HaRecorderTransport, HaServeConfig, HaStartupConfig, LogPeer, RecorderRpc,
};
use tokio::net::TcpListener;

async fn run_ha_node(
    startup: HaStartupConfig,
    recorders: Vec<(String, Box<dyn RecorderRpc>)>,
    log_peers: Vec<Arc<dyn LogPeer>>,
    shutdown: impl Future<Output = ()>,
) -> Result<(), String> {
    let recorder_listener = TcpListener::bind("127.0.0.1:7001")
        .await
        .map_err(|error| error.to_string())?;
    let service_listener = TcpListener::bind("127.0.0.1:7002")
        .await
        .map_err(|error| error.to_string())?;
    let node = startup.start(HaServeConfig::new(
        recorder_listener,
        service_listener,
        HaRecorderTransport::Http,
        recorders,
        log_peers,
    ));

    tokio::pin!(shutdown);
    let observed = tokio::select! {
        result = observe_ha_node(&node) => result,
        () = &mut shutdown => Ok(()),
    };
    let cleanup = node.shutdown().await.map_err(|error| error.to_string());
    preserve_observation_and_cleanup(observed, cleanup)
}

async fn observe_ha_node(node: &rhizadb::HaNode) -> Result<(), String> {
    let db = node.ready().await.map_err(|error| error.to_string())?;
    db.status().await.map_err(|error| error.to_string())?;
    node.monitor().await.map_err(|error| error.to_string())
}

fn preserve_observation_and_cleanup(
    observed: Result<(), String>,
    cleanup: Result<(), String>,
) -> Result<(), String> {
    match (observed, cleanup) {
        (Err(primary), Err(cleanup)) => {
            Err(format!("{primary}; HA shutdown also failed: {cleanup}"))
        }
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Ok(()), Ok(())) => Ok(()),
    }
}
```

Call `HaServeConfig::with_admin` or `with_tail_token` before `start` to add
those routes to the service listener. `rhizadb` owns the required
Recorder-before-runtime ordering, checkpoint restore, successor proof
installation, Recorder rehydration, runtime retry, and
`CheckpointCoordinator` construction. The embedding application supplies
provider configuration, prebound listeners, configuration-scoped Recorder and
log-peer clients, and its process shutdown signal. Dropping `HaNode` requests
best-effort shutdown but cannot report cleanup failures, so planned shutdown
must always await `HaNode::shutdown`.

Membership replacement moves restore and catch-up out of the Stop fence through
`HaSuccessorPrestageConfig::start_live`. The returned `HaSuccessorNode` keeps
the same process and prebound listeners from learner catch-up through active
service. It exposes liveness immediately and readiness only after the learner
matches the observed predecessor tip. After the application supplies one exact
`HaPredecessor`, the owner tails through that bound Stop, atomically adopts the
stage, starts the target runtime on the retained listener, proposes Activate,
and establishes the target checkpoint baseline before admitting writes.
`HaCertifiedTailSource` is the only transport boundary; retry cadence, proof
validation, Stop matching, activation, and shutdown remain facade-owned.

`execute_sql` and `query` expose typed SQL, `RETURNING`, consistency, and
persistent idempotency. HTTP routes and workspace tooling are secondary
adapters over the same SQL node service contracts.

## Kubernetes Deployment

Kubernetes provides stable process identity, DNS, secrets, and orchestration;
the runtime does not call Kubernetes APIs and receives no service-account
token.
Each configuration has a profile-scoped headless Service and StatefulSet named
`rhiza-<profile>-c<config_id>`. Stable ordinals map to `node-1` through `node-N`.
Membership accepts 3 through 7 members through a JSON bundle:

```json
{
  "config_id": 1,
  "members": [
    {
      "node_id": "node-1",
      "url": "http://rhiza-sql-c1-0.rhiza-sql-c1:8081",
      "log_url": "http://rhiza-sql-c1-0.rhiza-sql-c1:8080",
      "token": "secret"
    }
  ]
}
```

The bundle is mounted from an immutable Secret and selected with
`RHIZA_CONFIG_BUNDLE_FILE`. Peer tokens live inside the bundle Secret;
client/admin tokens and object-store credentials live in separate Secrets.
Never put any of those values in a ConfigMap.

### Recorder transport candidate

HTTP/JSON remains the default recorder transport. The opt-in
`tcp-postcard` candidate replaces the HTTP recorder listener for that process
and uses persistent, lane-separated plaintext TCP connections for QuePaxa
recorder calls. Rollback means restarting with the `http` selector; the two
recorder transports are not exposed together:

```bash
export RHIZA_RECORDER_TRANSPORT=tcp-postcard
export RHIZA_RECORDER_TCP_LISTEN=0.0.0.0:8082
```

Every bundle member must then include its in-cluster TCP address:

```json
{
  "node_id": "node-1",
  "url": "http://node-1:8081",
  "log_url": "http://node-1:8080",
  "recorder_tcp_addr": "node-1:8082",
  "token": "secret"
}
```

`tcp-postcard` provides no encryption or cryptographic peer authentication.
The HELLO token and all recorder traffic cross the network in plaintext; HELLO
still fences accidental node/configuration mismatches, but it is not a security
boundary. Use this transport only when the Kubernetes cluster network, nodes,
CNI, and workloads are inside one trusted boundary. The supplied renderer
accepts only the generated headless-Service DNS addresses and exposes port 8082
only on that cluster-internal Service; it does not create an Ingress, NodePort,
LoadBalancer, `hostPort`, or `hostNetwork` listener. Apply a namespace-level
default-deny NetworkPolicy in environments that run untrusted workloads.

Set `RHIZA_RECORDER_TLS=on` to use the server-authenticated TLS 1.3 variant of
the same framed Postcard protocol. TLS is off by default and does not fall back
to plaintext when enabled. In addition to `RHIZA_RECORDER_TCP_LISTEN`, it
requires readable certificate, private-key, and CA-bundle files:

```bash
export RHIZA_RECORDER_TRANSPORT=tcp-postcard
export RHIZA_RECORDER_TLS=on
export RHIZA_RECORDER_TCP_LISTEN=0.0.0.0:8082
export RHIZA_RECORDER_TLS_CERT_FILE=/run/secrets/rhiza/recorder-tls/tls.crt
export RHIZA_RECORDER_TLS_KEY_FILE=/run/secrets/rhiza/recorder-tls/tls.key
export RHIZA_RECORDER_TLS_CA_FILE=/run/secrets/rhiza/recorder-tls/ca-bundle.pem
```

Every bundle member must also set `recorder_tls_server_name` to the DNS name in
that member's certificate SAN. The Kubernetes renderer takes
`RHIZA_RECORDER_TLS_SECRET`, mounts its `tls.crt`, `tls.key`, and
`ca-bundle.pem` keys, and uses the exact ordinal headless-Service DNS names.
Because all Pods in one StatefulSet mount the same Secret, its server
certificate must cover every ordinal member name in that configuration.
Set `RHIZA_RECORDER_TLS=off` (the default) for plaintext TCP/Postcard; TLS
files, TLS server names, or a TLS Secret are rejected in that mode. TLS cannot
be enabled with the HTTP transport, and the removed `tcp-tls-postcard`
transport value is rejected so conflicting settings fail closed.

This is server-authenticated TLS, not mTLS. The encrypted HELLO exchange still
authenticates callers with configured peer tokens. It protects RecorderRpc
only; public APIs and log-fetch URLs keep their separately configured HTTP
security contract. HTTP/JSON remains the production default, and promotion of
either TCP variant still requires the documented multi-host durability,
reconnect, rollback, and soak gates.

## rhiza KV API and Rust SDK

The KV server profile exposes authenticated `/v1/kv/put`, `/v1/kv/delete`,
`/v1/kv/get`, and `/v1/kv/scan` JSON routes. Keys, values, cursors, and range
bounds use padded standard Base64. Mutations require a stable `request_id`; an
exact retry replays the stored result, while reuse with different bytes is a
conflict. Use the same request ID and body after a timeout because a timed-out
write may still commit.

Rust consumers select the isolated SDK feature:

```toml
[dependencies]
rhiza-client = { version = "0.1.0", default-features = false, features = ["kv"] }
```

`RhizaClient::kv_put`, `kv_delete`, `kv_get`, and `kv_scan` apply the same
bounded deadlines, exact-body retries, endpoint ordering, protocol version,
and Bearer authentication as the SQL client. Non-Rust consumers can call the
HTTP routes directly or use `rhiza kv put|get|scan|delete` from the KV CLI
release asset.

## rhiza sql API

`rhiza sql` executes admitted deterministic SQLite DDL and DML as replicated,
idempotent command batches. Every `/v1/sql/execute` request has a stable request
ID; all statements in the request run in one SQLite transaction and either all
apply at the agreed qlog index or none do. QSQL v2 returns a typed result for
each statement, including statement-level `rows_affected` and bounded typed
`RETURNING` rows. The result is persisted with the request ID, so an exact retry
replays the original result rather than executing the SQL again.

`/v1/sql/query` accepts one read-only statement and supports `local`, applied-
index, and quorum read-barrier consistency. QSQL v2 is only the client request
encoding. Replication uses the QWAL v3 envelope: one canonical envelope can contain 1 to 1,024
ordered successful receipts at one shared qlog anchor. Each public typed call is
still bounded to 256 members and 512 KiB of aggregate canonical encoded input.
The runtime's bounded FIFO group-commit queue can combine concurrent whole calls
into one physical page effect. Pending jobs have a fixed 32 MiB encoded-byte
budget; one active physical group is capped at 2 MiB and 1,024 members. The
runtime coalesces an eligible prefix of single-statement commands. Each member
runs under a savepoint, so a failed member is rolled back without
discarding earlier successes or preventing later members. Multi-statement
commands retain their one-command transaction boundary. The envelope binds the
ordered successful receipt subset, shared base and target digests, final page
images, and executor fingerprint into one canonical payload. An all-failed
batch proposes nothing.

Exact duplicates in one batch alias the first result. A stored exact retry
returns its original result and anchor; the same request ID with different
bytes is a conflict. If another payload wins the proposed slot, Rhiza applies
that winner, rechecks stored receipts, and prepares the remaining requests from
the new exact base. An effect that exceeds the 512 KiB command cap is retried
with a halved prefix until it fits or one command alone is rejected. Receipt and
request-ID duplicate validation uses pre-sized `HashSet`s in one pass rather
than rescanning every preceding member. QWAL v3, the current generation-5
control sidecar, and generation-5 `QSNP` snapshots require a clean installation:
older files and payloads fail closed, with no migration or rolling dual decoder.

Strict SQL durability is owned by the Recorder quorum: ACK waits until at least
2/3 Recorder WALs contain the complete QWAL and receipts behind their
platform-safe file sync. SQLite, its generation-5 control sidecar, and the file
qlog are non-durable, rebuildable local views. ACK still waits for SQLite apply
so local read-after-write is visible, but it does not wait for another
SQLite/control flush. Startup validates the SQLite/control pair and its tip
before readiness; damage or a tip behind the verified checkpoint quarantines
the complete local node directory, restores the checkpoint, and catches up the
exact Recorder tail. A quorum that cannot certify a mixed or missing tail fails
closed. QCMD segments currently have no deletion path, so Recorder command GC
cannot outrun verified checkpoint coverage.

Read-only SQL runs only against the selected local materialization, so it may
use nondeterministic and runtime-introspection functions such as `random()`,
`datetime('now')`, and `sqlite_version()`. Replicated writes may also use
nondeterministic SQLite functions because only the winning staging result is
replicated; followers never execute the SQL again.
Read execution is interrupted after five seconds; a timeout returns retryable
`503 resource_exhausted`, releases the SQLite connection, and does not change
node readiness.

Queries otherwise support SQLite's broad read families, including standalone
`VALUES`, `SELECT`/`EXPLAIN QUERY PLAN`, recursive CTEs, window functions, and
JSON scalar and table-valued functions. Direct `PRAGMA` queries are limited to
observational names: `foreign_key_check`, `foreign_key_list`, `index_info`,
`index_list`, `index_xinfo`, `integrity_check`, `quick_check`, `table_info`,
`table_list`, and `table_xinfo` may take an argument; `application_id`,
`collation_list`, `compile_options`, `data_version`, `encoding`,
`freelist_count`, `function_list`, `module_list`, `page_count`, `pragma_list`,
`schema_version`, and `user_version` are no-argument only. Assignments,
`database_list`, and other unlisted pragmas are rejected.

SQL parameters and result cells preserve SQLite `null`, `integer`, `real`,
`text`, and `blob` types. For example:

```bash
rhiza sql execute --url http://127.0.0.1:8080 \
  --request-id create-users \
  --sql 'CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)'

rhiza sql execute --url http://127.0.0.1:8080 \
  --request-id insert-ada \
  --sql 'INSERT INTO users(id, name) VALUES (?1, ?2)' \
  --params-json '[{"type":"integer","value":1},{"type":"text","value":"Ada"}]'

rhiza sql query --url http://127.0.0.1:8080 \
  --sql 'SELECT id, name FROM users WHERE id = ?1' \
  --params-json '[{"type":"integer","value":1}]' \
  --consistency read_barrier
```

Atomic multi-statement batches use the same authenticated JSON RPC directly.
The SQL runtime preflights a batch against the current agreed state before
proposing it, then replicates the resulting QWAL page effect. DDL, triggers,
foreign-key cascades, ROWID/AUTOINCREMENT behavior, nondeterministic functions,
indirect changes, and bounded `RETURNING` results are supported. Operations
that escape the state-machine boundary remain rejected: direct `__rhiza_*`
access, replicated-write `PRAGMA`, `ATTACH`/`DETACH`, TEMP objects, virtual
tables, extension loading, and explicit transaction/savepoint control. Query
and `RETURNING` responses are bounded by server row and byte limits.

SQLite storage is QWAL-only. A canonical user database and its mandatory
`.control` sidecar form one current-layout pair; open and recovery validate
their shared identity and reject incomplete, mismatched, or noncurrent layouts
before mutation. Same-generation `QSNP` restore is recovery, not a format
conversion path.
The recording VFS currently runs in staging shadow/audit mode, while full
closed-file page diff remains the correctness path. Preparation computes the
target digest during that same complete target scan. Apply-time base/target
digest validation, file sync, owned-inode checks, atomic rename, parent-directory
sync, and receipt/control commit remain unchanged.

Recovery metadata has semantic `RecoveryAnchor` format v2, serialized in the
binary QANC v4 envelope. It binds the recovery generation, configuration state,
snapshot identity, and executor fingerprint; a mismatch is rejected during
recovery rather than replayed best-effort.

The message-level comparison with Hiqlite and the bounded effect-replication
contract are documented in [`docs/hiqlite-sql-message-review.md`](docs/hiqlite-sql-message-review.md).
The measured 3-peer `emptyDir` / no-PVC failure matrix and operator recovery
steps are documented in
[`docs/three-peer-emptydir-recovery-2026-07-19.md`](docs/three-peer-emptydir-recovery-2026-07-19.md).

## Storage Model

All StatefulSets, including prestaged successors, use bounded `emptyDir`
storage. Before predecessor scale-down, a recreated successor rebuilds its
detached stage from the predecessor checkpoint and certified tail. Predecessors
remain available until the first Active successor checkpoint is independently
verified. After that checkpoint, ordinary rejoin restores the target
configuration from object-store authority. No Kubernetes PVC is part of the HA
correctness or performance contract.
The local vind RustFS Deployment also uses `emptyDir`; it simulates S3
compatibility but is not production durability evidence.

The runtime uses the generic `object_store` boundary. The deployment
template is S3-shaped for RustFS, AWS S3, or another compatible endpoint;
runtime support also includes GCS and Azure Blob through provider configuration.
No provider or Kubernetes API appears in consensus logic.

Remote checkpoint V2 stores the selected engine snapshot as opaque bytes plus
its identity, applied index/hash, configuration state, and materializer
fingerprint. Restore validates that envelope, rebinds the materializer to the
target node for cross-node recovery, installs it in a fresh data directory,
and replays the exact committed suffix after the snapshot. SQLite files are
never treated as an independent recovery authority.

An application write does **not** perform an S3 CAS. Writes first commit through
QuePaxa and append locally. Checkpoint publication batches state into immutable
objects and conditionally advances a small manifest. `sync` durability may
publish for every acknowledged write to provide RPO0, while `bounded` and
`periodic` modes reduce object-store traffic with the documented lag tradeoff.

## Local Checks

```bash
cargo test
shellcheck scripts/*.sh
bash -n scripts/*.sh
yq eval '.' deploy/k8s/*.yaml >/dev/null
scripts/check-deploy.sh
```

Render a config-scoped StatefulSet without writing the bundle token to a YAML
artifact. Initialize the authoritative object-store checkpoint before creating
the StatefulSet; all rendered pods start in `rejoin` mode and restore from that
checkpoint:

```bash
export RHIZA_EXECUTION_PROFILE=sql
export RHIZA_K8S_NAMESPACE=rhiza
export RHIZA_IMAGE=rhiza-sql:dev
export RHIZA_CLUSTER_ID=cluster-a
# Point these at the object store and existing credential Secret for this namespace.
export RHIZA_S3_ENDPOINT=https://s3.example.internal
export RHIZA_OBJECT_SECRET=rhiza-object-store

kubectl -n "$RHIZA_K8S_NAMESPACE" create secret generic rhiza-sql-c1-bundle \
  --from-file=config.json=config-c1.json --dry-run=client -o yaml \
  | yq eval '.immutable = true' - \
  | kubectl -n "$RHIZA_K8S_NAMESPACE" create -f -
scripts/k8s-object-job.sh 1 config-c1.json init-checkpoint
kubectl -n "$RHIZA_K8S_NAMESPACE" apply -f deploy/k8s/rhiza-client-services.yaml
scripts/render-k8s-config.sh 1 3 config-c1.json target/rhiza-sql-c1.yaml
kubectl -n "$RHIZA_K8S_NAMESPACE" create -f target/rhiza-sql-c1.yaml
```

`rhiza-sql-client` is the single stable SQL client Service. It is installed
once from the static manifest, is never emitted or mutated by the
configuration renderer, and selects only Pods labeled
`rhiza.dev/member-role=voter`. The rendered, config-scoped headless Service
remains the peer and direct admin path; it deliberately has no role selector so
an awaiting-activation learner can be inspected without becoming a client
endpoint. Configuration replacement verifies the live stable Service has this
exact selector and client port before it performs any transition action.

The renderer derives the local image default from the selected profile
(`sql` uses `rhiza-sql:dev`, `graph` uses `rhiza-graph:dev`, and `kv` uses
`rhiza-kv:dev`). Set `RHIZA_IMAGE`
to override it with a registry-qualified artifact and tag. Also set
`RHIZA_CLUSTER_ID`, `RHIZA_EPOCH`, `RHIZA_RECOVERY_GENERATION`, `RHIZA_S3_*`,
and Secret-name overrides as needed. `RHIZA_EXECUTION_PROFILE` must be
`sql|graph|kv`.
The example assumes `rhiza-auth` and `rhiza-object-store` Secrets already
exist in the same `rhiza` namespace. `rhiza-auth` must contain distinct
`client-token`, `admin-token`, and `tail-token` values; replace the image,
endpoint, and Secret names with deployment-specific values.
The renderer accepts only an unset or explicit `rejoin`
`RHIZA_STARTUP_MODE`; bootstrap and disaster-recovery startup modes are not
valid for the reference StatefulSet.

The rendered Rhiza workload is compatible with distroless images: Kubernetes
invokes the `rhiza` entrypoint directly and the binary derives `node-N` from
the trailing StatefulSet ordinal in `HOSTNAME` when `RHIZA_NODE_ID` is unset.
An explicit nonempty `RHIZA_NODE_ID` remains authoritative; a missing or
malformed `HOSTNAME` fails startup instead of silently selecting an identity.
Pods run as UID/GID 65532 with a read-only root filesystem, dropped Linux
capabilities, `RuntimeDefault` seccomp, and writable `emptyDir` mounts only for
Rhiza data and `/tmp`.

Graph and KV rendering, checkpoint jobs, readiness checks, and replacement
helpers use the same profile-scoped contracts. The static stable client Service
remains SQL-only; Graph and KV clients target the rendered profile-scoped
Service directly. Graph and KV Kubernetes deployment remains beta pending a
published multi-node cluster smoke result; their runtimes, checkpoint restore,
isolated binaries, and container builds are supported.

The ordinary rendered StatefulSet uses `Parallel`, `OnDelete`, stable ordinals,
and per-Pod `emptyDir` data. If one Pod is deleted or lost, Kubernetes
recreates the same ordinal; a container-only crash restarts in the existing
Pod. A replacement restores the initialized official checkpoint plus its
committed suffix. No scale, configuration, or recovery command is part of this same-membership
repair.

The matching PodDisruptionBudget sets `maxUnavailable: 1` to limit cooperating
voluntary disruptions. A PDB does not prevent direct Pod deletion, process or
node failure, involuntary eviction, or simultaneous failures, and it does not
repair quorum by itself.

This automatic repair is not a membership change and is not a general binary
upgrade guarantee. Changing member identities still uses the stop-and-replace
flow below. `OnDelete` also means an image/template change does not roll pods
automatically; only deploy mutually compatible binaries and verify them one
ordinal at a time under a separate upgrade procedure.

The node-bound local identity marker is
`.rhiza-checkpoint-identity.json`; it includes
the exact node identity as well as the cluster, profile, epoch, configuration,
and recovery generation.

Interrupted checkpoint replacement uses `.rhiza-restore.json`, bound to the
same identity plus the exact checkpoint index and hash. Other local
restore-intent artifacts fail without replacing local state.

On steady-state `rejoin`, a missing local Recorder is rebuilt behind the
startup gate from the verified qlog and quorum-certified peer tail before the
Pod becomes Ready. A partial, corrupt, or foreign Recorder fails closed.
Successor configurations may additionally reconstruct a missing Recorder from
their completed node-bound receipt and predecessor Stop proof. Rebuildable
SQLite and qlog state may be quarantined and restored independently; Recorder
state is never included in that quarantine.

## Stop And Replace

Membership replacement still has a logical QuePaxa Stop(S) → Activate(S+1)
barrier, but bulk restore, tail catch-up, target archive initialization, and
listener startup happen before Stop. The remaining write gap contains only the
exact Stop tail, local adoption, runtime opening, Activate, and first target
checkpoint publication. The bounded operator flow is:

1. Prepare a successor draft with config ID S+1 and 3 through 7 members.
2. Create learner-labeled successor Pods before Stop. Their main process runs
   `rhiza prestage serve`, restores Active(S), continuously consumes
   authenticated certified tail pages, and reports Ready only when caught up.
   The facade pre-initializes the target archive namespace at this stage.
3. Call old live admin `membership/stop`, poll every old node to exact
   `Stopped(S)`, and bind the returned Stop proof into an immutable transition
   Secret. The same process tails to the exact Stop, adopts its staged state,
   starts the target runtime on the retained sockets, auto-activates only from
   that Stop capability, and publishes the first Active(S+1) checkpoint.
4. Poll every successor until it reports exact `Active(S+1)`. There is no
   init-to-main restart and no separate operator activation request.
5. Promote exact Active successor Pods to voter, relabel the stopped
   predecessor Pods as sealed, and require the stable client Service
   EndpointSlices to contain exactly the Ready successor Pod UIDs.
6. Publish and inspect the first Active(S+1) checkpoint. Only after that proof
   scale the sealed predecessor StatefulSet to zero and permit GC.

The temporary coexistence is safe because pre-Stop successors are recovery-only
learners and cannot propose or accept writes, while predecessor Pods cannot
write after exact `Stopped(S)`. Keeping sealed predecessors until the first
Active(S+1) checkpoint preserves rollback evidence without admitting them to
the stable voter-only client Service.

For a first upgrade from a manifest that predates member-role labels, the
replacement preflight first proves every exact old node is `Active(S)`. It then
validates the old StatefulSet and complete owned Running Pod set before
idempotently adopting the voter label. Missing labels are accepted only through
this evidence-backed path; learner or foreign labels, owners, identities, or
Pod replacements fail closed before UID evidence is captured.

Run the guarded workflow with:

```bash
RHIZA_KUBE_CONTEXT=my-vind-context \
RHIZA_K8S_NAMESPACE=rhiza-e2e \
RHIZA_EXECUTION_PROFILE=sql \
scripts/replace-k8s-config.sh config-c1.json config-c2-draft.json
```

The live-admin routes share the client listener but have a distinct bearer
token and operation contract. Defaults are under `/v1/admin`; path variables on
the script allow a staged API rename without weakening response validation.
Every Job has an active deadline and `backoffLimit: 0`. Poll loops are bounded;
elapsed time and sleeps never establish correctness. Only observed node,
checkpoint, StatefulSet, and object-store state advances the workflow. Any
missing, malformed, mismatched, or timed-out observation aborts the operation.

## Checkpoint And GC

Node-local checkpoint compaction is a live-admin operation. Object-wide
checkpoint inspection and GC use a short-lived `rhiza` CLI Job with generic
object-store credentials. Examples:

```bash
export RHIZA_EXECUTION_PROFILE=sql
scripts/k8s-object-job.sh 2 config-c2.json checkpoint inspect

plan_json="$(scripts/gc-k8s.sh plan config-c2.json)"
plan_hash="$(jq -r .plan_hash <<<"$plan_json")"
scripts/gc-k8s.sh inspect config-c2.json "$plan_hash"
RHIZA_GC_CONFIRM_PLAN_HASH="$plan_hash" \
  scripts/gc-k8s.sh apply config-c2.json "$plan_hash"
```

`gc plan` is non-destructive and persists an identity-bound plan. `gc inspect`
must retrieve the same 64-character lowercase SHA-256 hash. `gc apply` refuses
to run unless the operator supplies that exact hash both as the argument and in
`RHIZA_GC_CONFIRM_PLAN_HASH`; the CLI also requires `--confirm`. Plans remain
subject to generation retention, grace, and minimum-age policy. Never delete
objects by prefix or bypass the plan evidence: manifests, snapshots, suffixes,
and a concurrently referenced old generation can otherwise be lost.

## Vind E2E

The local harness requires Docker, `kubectl`, `vcluster` (vind), `jq`, `yq`, and
OpenSSL:

```bash
scripts/e2e-vind-rustfs.sh
```

It creates a fresh namespace and RustFS bucket, boots config 1 on `emptyDir`,
writes snapshot and suffix data, compacts to checkpoint V2, performs a 3-to-3
zero-PVC prestaged stop-and-replace, proves recovery by successful reads, then
plans, inspects, and applies old-object GC with exact
hash confirmation after stopping publishers and observing lease expiry. It
restarts the three nodes and verifies the retained generation afterward.
Cleanup is automatic by default. Set
`RHIZA_VIND_CLEANUP=0` to retain the cluster for diagnostics.

The benchmark client can keep serving writes while one node is replaced by
opening all three node endpoints and retrying only transport failures and
retryable HTTP responses. The request body and `request_id` are unchanged on
every attempt, so the persistent idempotency record remains the correctness
boundary. To measure deletion of the preferred proposer (`ordinal 0`):

```bash
RHIZA_BENCH_MULTI_ENDPOINT=1 \
RHIZA_BENCH_RESOURCE_SAMPLING=0 \
RHIZA_DURABILITY_MODE=periodic \
RHIZA_DURABILITY_INTERVAL=1s \
scripts/bench-vind.sh \
  --duration 60s --warmup 5s --concurrency 4 --workload write \
  --fault pod-delete --fault-offset 10s --fault-pod rhiza-sql-c1-0
```

RustFS remains an object-storage simulator in this harness. The fault command
targets only a `rhiza sql` pod; it does not inject RustFS failures.

The implemented fast-path, microbatch, failover, and OSS cost results are in
[docs/failover-throughput-optimization-2026-07-12.md](docs/failover-throughput-optimization-2026-07-12.md).
The current Recorder durability, typed-batch, and production-adapter transport
evidence is in
[docs/performance-optimization-2026-07-17.md](docs/performance-optimization-2026-07-17.md).
Its Linux WAL syscall comparison is reproducible with
[`bench/run-recorder-sync-linux.py`](bench/run-recorder-sync-linux.py) and
[`bench/support/fdatasync-as-fsync.c`](bench/support/fdatasync-as-fsync.c); the
auditable 12-pair artifacts are tracked as
[`raw.jsonl`](docs/benchmarks/recorder-sync-linux-20260717/raw.jsonl) and
[`summary.json`](docs/benchmarks/recorder-sync-linux-20260717/summary.json).
The 24-row raw artifact is about 48.6 KiB and the summary is about 9.4 KiB. That
run used a dirty worktree and Docker Desktop's virtual filesystem, so the
summary sets `production_valid=false`. Native `fdatasync` had 1.561x aggregate
median throughput and lower aggregate p50/p95/p99. However, the paired
`fsync/native` median was 0.928 and the win split was 6/12 each, so paired
performance remains inconclusive. Linux `sync_data` remains a
correctness-preserving candidate for the smaller durability syscall, not a
production speedup claim. Production adoption requires clean physical
crash/reopen and throughput/latency validation on the target
ext4/XFS/Kubernetes CSI stack.
The primary-source protocol conformance and performance-comparability limits are
in [docs/quepaxa-paper-conformance-2026-07-12.md](docs/quepaxa-paper-conformance-2026-07-12.md).

## Deferred Performance Tuning

MAB-based preferred-proposer and hedge-delay auto-tuning is deliberately **not
implemented**. Its safety boundary, bounded action space, fallback behavior,
telemetry, and staged rollout requirements remain documentation-only in
[docs/mab-leader-hedge-tuning.md](docs/mab-leader-hedge-tuning.md).
