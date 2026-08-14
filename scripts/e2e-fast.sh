#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

max_seconds="${RHIZA_FAST_E2E_MAX_SECONDS:-90}"
case "$max_seconds" in
  ''|*[!0-9]*|0) echo "RHIZA_FAST_E2E_MAX_SECONDS must be positive" >&2; exit 64 ;;
esac

command -v cargo >/dev/null 2>&1 || {
  echo "cargo is required" >&2
  exit 69
}

run() {
  printf '+ '
  printf '%q ' "$@"
  printf '\n'
  "$@"
}

# This is the local inner loop, not release qualification. It deliberately
# avoids Docker, Kubernetes, cloud object storage, and Chaos Mesh while still
# exercising the two process boundaries that have caused the live failures:
# three real postcard-rpc/TCP Recorder servers and compacted-checkpoint rejoin.
#
# The budget measures test execution only, not compilation. Pre-build the
# exact test targets so a cold runner (GitHub CI) does not consume the budget
# with cargo downloads and linking.
run cargo test --locked -p rhiza-cli --features recorder-postcard-rpc --bin rhiza \
  --no-run
run cargo test --locked -p rhiza-node --features sql --lib \
  --no-run

started_at="$(date +%s)"

run cargo test --locked -p rhiza-cli --features recorder-postcard-rpc --bin rhiza \
  tests::staggered_postcard_rpc_cluster_commits_after_all_recorders_start -- --exact
run cargo test --locked -p rhiza-node --features sql --lib \
  durability::tests::shared_checkpoint_ahead_of_local_qlog_satisfies_the_local_flush -- --exact
run cargo test --locked -p rhiza-node --features sql --lib \
  tests::recorder_unknown_outcome_is_retryable_without_killing_runtime -- --exact

elapsed="$(( $(date +%s) - started_at ))"
if [ "$elapsed" -gt "$max_seconds" ]; then
  echo "fast E2E exceeded ${max_seconds}s budget: ${elapsed}s" >&2
  exit 1
fi
printf 'fast E2E passed in %ss (budget %ss)\n' "$elapsed" "$max_seconds"
