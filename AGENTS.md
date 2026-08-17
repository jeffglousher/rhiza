# Repository Agent Instructions

## Verification Layers

Use the smallest verification layer that matches the change, and escalate as
the change approaches release. A higher layer does not remove the need for the
lower, faster feedback loops.

1. After each relevant code change, run `scripts/e2e-fast.sh`. It is the
   process-level inner loop and should complete in seconds with a warm Cargo
   target.
2. Before opening or updating a pull request, run `scripts/check-ci.sh`. This
   is the complete local CI suite and must pass before relying on GitHub CI.
3. For changes that cross deployment, networking, storage, recovery, or
   multi-process integration boundaries, test against a reusable local
   Kubernetes cluster. Reuse is for development speed and is not fresh-cluster
   release evidence.
4. For a release candidate, use a fresh Kubernetes namespace and storage
   namespace, real GCS, and Chaos Mesh. Preserve immutable image digests,
   workload results, recovery/read-barrier evidence, cluster state, and
   artifact checksums.

Do not claim Kubernetes, GCS backup/restore, or Chaos qualification from
`scripts/e2e-fast.sh` or unit tests. Do not run fresh cloud qualification for
ordinary edit/test iterations when a lower layer can detect the regression.
