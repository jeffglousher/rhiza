# Chaos durability validation

This is the reproducible replacement for the unavailable physical-hardware
power-loss experiment. It deliberately makes a narrower claim: it validates
Rhiza recovery under the listed software fault models. Every result must state
`physical_power_loss: false`; it cannot establish device-cache, firmware, or
actual mains-removal durability.

## Two isolated lanes

The Kubernetes lane runs only on a disposable Linux cluster whose context and
namespace are explicitly named `rhiza-chaos-*`. It refuses `default`, a broad
selector, automatic namespace creation, and a context outside that prefix.
Its workload pods must carry all three labels:

```yaml
app.kubernetes.io/part-of: rhiza-chaos
chaos.rhiza.io/run: <unique-run>
chaos.rhiza.io/role: node-0 # node-1 or node-2
```

Run `pod-kill` for a non-quorum or preferred-proposer loss, then
`network-partition` from one role to the other. `io-error` is optional and is
only valid on the same disposable Linux lane. Rotate the affected ordinal and
repeat each case at least three times. A failed or unproven injection is an
invalid experiment, not a passing recovery result.

The VM lane is external to the guest and Linux-only: the controller launches
QEMU with the same canonical disk image, verifies `/proc/<pid>/exe` and the
process start identity against the canonical `QEMU_BINARY`, sends `SIGKILL`
through a Linux pidfd, waits on that pidfd, then reboots using that exact disk
path. Every hook attests the
disk path (and launch attests its pre-run SHA-256). It models abrupt VM loss only. It must not
be described as a physical power cut.

## ACK boundaries and artifacts

Both lanes run separate `pre-ack` and `post-ack` trials. The workload hook
emits `chaos-workload-boundary` JSON with exactly one Rhiza and one Hiqlite
entry, unique IDs, the real `ack_kind`, a boolean durable-ACK observation, and
`fault_observed: true` after injection. A pre-ACK verification has no durable
ACK; a post-ACK verification has one for both entries. Rhiza's normal
comparison is `object-authoritative-sync`; use Hiqlite only with its equivalent
durable ACK and matching RPO/fsync/log policy. Preserve all attempts, including
inconclusive ones.

`scripts/chaos-k8s.sh run OUTPUT ...` captures the rendered CRD, injection
status, hook captures, `kubectl version`, and a SHA-256 manifest. Before
workload verification it requires Chaos Mesh 2.8.2 `Selected=True`,
`AllInjected=True`, and positive aggregate `containerRecords[].injectedCount`;
a no-op CRD is invalid. The launch,
reboot, and verification hooks of `scripts/chaos-vm-loss.sh` capture the same
boundaries and record the exact `QEMU_BINARY`, QEMU PID, disk identity, and
controller kill. The manifests classify faults as `process_kill`,
`network_chaos`, `io_fault`, or `vm_abrupt_loss` and include target roles and
timestamps where applicable. These artifacts
are provenance, not proof that a physical device lost power.

## Chaos Mesh supply-chain gate

The repository pins Chaos Mesh **2.8.2** in
[`deploy/chaos/chaos-mesh.lock.json`](../deploy/chaos/chaos-mesh.lock.json).
The lock records the official chart SHA-256 and the four images rendered by the
2.8.2 default chart: controller manager, daemon, dashboard, and DNS server.
`install` downloads the chart afresh, hashes it, renders it, then uses the
repository post-renderer to replace those exact tags with the reviewed
multi-platform registry digests before applying the rendered objects. It
refuses any render whose image set differs from the lock.
If custom values add an image, update the lock and renderer review first. Run:

```sh
CHAOS_DISPOSABLE_CLUSTER=I_UNDERSTAND_DISPOSABLE_CHAOS \
CHAOS_K8S_CONTEXT=kind-rhiza-chaos-lab CHAOS_NAMESPACE=rhiza-chaos-lab \
scripts/chaos-k8s.sh install approved-chaos-mesh-values.yaml
```

No live chaos command belongs in CI. CI validates syntax, exact version,
fail-closed digest state, narrowed selectors, and the explicit
`physical_power_loss: false` contract.
