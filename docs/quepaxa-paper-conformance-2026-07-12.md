# QuePaxa Paper Conformance and Performance Boundaries

Date: 2026-07-12

Evidence status: benchmark figures in this document are historical observations
from unversioned local runs. The referenced `target/rhiza-bench/...`
directories are not included in the repository and cannot be audited from a
checkout.

Primary source: Tennage et al., [*QuePaxa: Escaping the Tyranny of Timeouts in
Consensus*](https://discovery.ucl.ac.uk/id/eprint/10181480/1/quepaxa.pdf), SOSP
2023.

## Protocol conformance

| Paper property | rhiza sql status |
|---|---|
| A designated first-round proposer can decide after phase 0, one recorder round trip, when its highest-priority proposal reaches a quorum. | Rhiza forms a FastPath decision proof after that phase-0 recorder quorum. This is a protocol decision point, not the current public acknowledgement boundary: the normal success path then installs the proof durably on a recorder quorum. Rhiza therefore does not claim a one-RTT client-visible FastPath. |
| If the first round does not decide, round 2 and later are leaderless and fully asynchronous. Each round has four phases and succeeds probabilistically rather than through a timeout-driven view change. | The driver continues through four-phase rounds. Its proposal loop has cancellation but no fixed retry/round cap; phase progress is not a timeout-based liveness proof. |
| Hedging and leader selection reduce redundant work but are not safety or liveness requirements. The paper adapts both with MAB-style tuning. | rhiza sql currently uses a static preferred identity and preferred-first endpoint order; later endpoints are used only for fallback or hedge attempts. MAB leader/hedge tuning remains documentation-only in [`mab-leader-hedge-tuning.md`](mab-leader-hedge-tuning.md). |

Rhiza adds a durable-publication boundary beyond the paper's protocol decision
point. On the normal success path, FastPath and Phase2 decisions, including
`ConfigChange` decisions and decisions reached after a transition was observed,
install a durable proof on a recorder quorum before the public proposal API
returns success. If installation
reports an ambiguous failure, the API may still acknowledge only after a bounded
inspection certifies the exact matching committed value; otherwise it returns
`UnknownOutcome`, while conflicting safety evidence remains terminal. Recovery
may reconstruct FastPath from recorder summaries, but that capability is not an
unconditional live acknowledgement shortcut. Recovery never reconstructs
Phase2 from summaries alone; it requires an installed decision proof.

## Executable safety evidence and boundaries

`crates/rhiza-quepaxa/tests/adversarial_agreement.rs` is deterministic executable
evidence for a bounded set of agreement schedules; it is not a formal proof or
whole-system verification. For fixed memberships of 3, 5, and 7 recorders, it
keeps exactly a quorum live, drops every recorder RPC to the remaining minority,
duplicates live record delivery, and replays a previously delivered lower-step
request after a newer request on a separate probe slot. It then commits command A
on the ordinary fast path, verifies that a later conflicting proposer B adopts the
same `LogEntry`, closes the in-process recorder handles, reopens the live quorum,
and reconstructs the same certified decision while the minority remains
unavailable. A seeded `PrioritySource`, synchronization barriers, thread joins,
and a channel watchdog timeout make that schedule repeatable without timing
sleeps in the test controller.

Residual gaps include:

- concurrent proposer interleavings (the executable schedule orders proposer A
  before conflicting proposer B);
- dynamic membership and configuration-transition schedules;
- `NodeRuntime`, qlog, and materializer integration;
- real network transports, partitions, retransmission, and process failure;
- Byzantine recorders or proposers; and
- physical power loss and storage-device durability behavior.

## Performance comparability

The paper reports about 584k commands/s in LAN and 250k commands/s in WAN for
its Go SMR prototype. Those are 17-byte string-KV commands (one-byte operation,
eight-byte key, and eight-byte value), not SQLite transactions over an OSS HTTP
service. Its evaluation batches at both submitters and proposers. Its LAN
optimization broadcasts batch contents from submitters and orders compact batch
IDs; its WAN evaluation additionally pipelines ten slots.

The current rhiza sql benchmark instead measures HTTP SQL transactions, including
one parameterized SQLite `INSERT` per request and rhiza sql's storage/durability
path. The measurements retained in
[`failover-throughput-optimization-2026-07-12.md`](failover-throughput-optimization-2026-07-12.md)
support comparisons between the recorded rhiza sql runs only. They do not prove
the paper's throughput, parity with its prototype, or Raft-equivalent
performance.

## Reported local rhiza sql observation

The local notes for `target/rhiza-bench/20260712-113825-51624` report a
30-second, concurrency-4, periodic-1-second run with one SQL `INSERT` per
request and preferred-first multi-endpoint routing. They report all 9,143
attempts completed with zero errors at 304.77 transactions/s (p50 12.8 ms,
p95 51.2 ms, p99 102.4 ms).

The same notes report 317.4 transactions/s before preferred-pod deletion and
298.316 transactions/s with zero errors during the 20.009-second
deletion/restore window, about a 6.0% reduction. They report that the fault
command succeeded in 34.846 seconds, checkpoint drain waited zero seconds, qlog
and checkpoint ended at index 5193 with the same hash, and the run used 5,259
OSS requests while retaining 29,855,178 bytes in 286 objects.

This unversioned observation is scoped to the stated rhiza sql workload and
topology only. It is neither a reproduction of the paper workload nor evidence
of Raft-equivalent performance.

## Missing performance evidence and work

- Run rhiza sql and Raft on the same command schema, payload size, batching,
  durability, topology, load model, and latency definition before claiming
  Raft-equivalent performance.
- Add and evaluate a paper-grade data path in which submitters disseminate
  batches, consensus orders batch IDs, and missing batch contents are fetched
  before recorder acknowledgement.
- Add bounded multi-slot pipelining and evaluate it separately, especially for
  WAN operation.

Until those exist, the 584k/250k paper results are context and targets, not
rhiza sql benchmark results.
