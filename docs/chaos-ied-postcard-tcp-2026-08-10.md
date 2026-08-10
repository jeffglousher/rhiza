# IED postcard/TCP chaos qualification — 2026-08-10

## Scope

- GKE context: `gke_patch2-the-new-era_asia-northeast3_ied-cluster`
- Disposable namespace: `rhiza-chaos-ied-20260810-c84f0a`
- Run ID: `ied-c84f0a`
- Recorder transport: `tcp-postcard-rpc`
- Image: `ghcr.io/mrchypark/rhiza-sql@sha256:64603d2b4f678cece820197f4beb00cff4deaad4a398594078905a70731632ba`
- Image source commit: `cf2db7eda29e65188bea17d748792075aaa23069`
- Image build: GitHub Actions run `31360079310`, job `93367095465`
- Chaos engine: Chaos Mesh 2.8.2, digest-pinned chart and images

The namespace used three Rhiza SQL voters and one ephemeral RustFS object-store deployment. Workloads were labelled before their first creation, so the test did not restart an `emptyDir`-backed workload merely to add Chaos Mesh selectors.

## Results

| Scenario | Pre-ACK | Post-ACK | Result |
| --- | --- | --- | --- |
| Voter Pod termination | Recovery and read verification passed | Durable ACK was observed, the selected voter was terminated, three-voter read-barrier recovery completed, and the acknowledged value was verified | PASS |
| Bidirectional voter ↔ object-store partition | Read verification passed while the fault was active | Durable ACK was observed and the acknowledged value was verified while the partition was active | PASS |
| Voter data-path I/O error | Prepare completed | Chaos Mesh selected a voter but injected no fault | NOT EXECUTED |

The successful manifests are stored locally under:

- `target/chaos-ied/20260810-c84f0a/scenarios/pod-kill/manifest.json`
- `target/chaos-ied/20260810-c84f0a/scenarios/network-partition/manifest.json`

Both manifests bind the scenario files and workload hook by SHA-256. Their post-ACK verification records use `ack_kind: consensus-and-proof-quorum` and set `durable_ack_observed: true`.

The I/O scenario failed closed before application validation. Chaos Mesh reported `AllInjected=False`, `injectedCount=0`, and:

```text
toda startup takes too long or an error occurs: Read-only file system (os error 30)
```

The selected voter remained Ready. The failed IOChaos and child PodIOChaos objects were removed after confirming that no injection had occurred. This result is an infrastructure limitation of the tested GKE/container-runtime combination, not a Rhiza I/O-fault pass or failure.

## Defects found and fixed

1. Applying chaos labels after deployment rolled the RustFS Deployment and erased its ephemeral `emptyDir`. The deployment path now injects all chaos labels before the first create/apply.
2. Genesis checkpoint restore reconstructed configuration state with `LogHash::ZERO` instead of the checkpoint identity's configuration digest. A restored QEFX suffix therefore failed verification after voter termination. Restore now uses the checkpoint digest, with a regression test.
3. The bounded recovery waiter aborted on a transient `kubectl` error. It now records and retries transient observation failures until its deadline.

## Verification and limits

Focused restore tests, the new configuration-digest regression, strict Rhiza Node Clippy, the chaos static checker, and diff checks passed. The SQL image was built by GitHub Actions and deployed by immutable digest; Google Cloud Build was not used.

The full Rhiza Node durability test target has one unrelated, deterministic pre-existing failure: `cancelled_recorder_rehydration_is_joined_without_late_persistence` returns `Unavailable("QuePaxa quorum was not reached")`. It also fails in isolation and predates the checkpoint-digest fix.

This run is a Kubernetes process/network chaos qualification. It is not evidence of physical power-loss behavior. The I/O fault class remains unqualified until it can be injected on a compatible disposable worker/runtime or an equivalent VM crash/disk harness.
