#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

required_commands=(cargo yq)
for command_name in "${required_commands[@]}"; do
  command -v "$command_name" >/dev/null 2>&1 || {
    echo "required CI tool is missing: $command_name" >&2
    exit 69
  }
done

packages=(
  rhiza-core rhiza-log rhiza-obj-store rhiza-quepaxa rhiza-archive
  rhiza-sql rhiza-graph rhiza-kv rhiza-node rhizadb rhiza-client
  rhiza-cli rhiza-tuner
)
package_args=()
for package in "${packages[@]}"; do
  package_args+=(-p "$package")
done

run() {
  printf '+ '
  printf '%q ' "$@"
  printf '\n'
  "$@"
}

run cargo fmt --all -- --check
run cargo build --release --locked -p rhiza-cli --bin rhiza --features recorder-postcard-rpc
run cargo clippy --locked --all-targets "${package_args[@]}" -- -D warnings
run cargo test --locked --all-targets "${package_args[@]}"
run cargo test --locked -p rhiza-node --features graph --lib \
  graph_write_retries_the_same_request_after_unknown_recorder_outcome

for feature in shadow proposer-canary hedge-canary default-on; do
  run cargo test --locked -p rhiza-tuner --no-default-features --features "$feature" \
    rollout::tests::legacy_feature_default_is_preserved --lib
done

run cargo test --locked -p rhiza-quepaxa --features test-hooks
run cargo fmt --manifest-path bench/Cargo.toml -- --check
run cargo clippy --manifest-path bench/Cargo.toml --locked --all-targets -- -D warnings
run cargo test --manifest-path bench/Cargo.toml --locked --all-targets
run cargo fmt --manifest-path bench/hiqlite-recovery-client/Cargo.toml -- --check
run cargo clippy --manifest-path bench/hiqlite-recovery-client/Cargo.toml --locked --all-targets -- -D warnings
run cargo test --manifest-path bench/hiqlite-recovery-client/Cargo.toml --locked --all-targets

run bash -n \
  scripts/bench-vind.sh \
  scripts/bench-rhiza-hiqlite.sh \
  scripts/bench-rhiza-hiqlite-steady.sh \
  scripts/bench-hiqlite-steady.sh \
  scripts/bench-power-loss-durability.sh \
  scripts/chaos-k8s.sh \
  scripts/chaos-vm-loss.sh \
  scripts/check-bench-rhiza-hiqlite-static.sh \
  scripts/check-bench-rhiza-hiqlite-steady-static.sh \
  scripts/check-power-loss-durability-static.sh \
  scripts/check-chaos-static.sh \
  scripts/tuner-monitor.sh \
  scripts/check-tuner-monitor-static.sh \
  scripts/check-workspace-packages.sh \
  scripts/check-quepaxa-package.sh \
  scripts/check-ci.sh

run scripts/check-bench-vind-static.sh
run scripts/check-bench-rhiza-hiqlite-static.sh
run scripts/check-bench-rhiza-hiqlite-steady-static.sh --fixture
run scripts/check-power-loss-durability-static.sh
run scripts/check-chaos-static.sh
run scripts/check-storage-format-compatibility.sh
run scripts/check-tuner-monitor-static.sh
run scripts/check-deploy.sh
run scripts/check-workspace-packages.sh
if git diff --quiet --ignore-submodules -- && git diff --cached --quiet --ignore-submodules --; then
  run scripts/check-quepaxa-package.sh
else
  # Local pre-commit verification must package the exact working tree under test.
  # CI remains clean and therefore exercises cargo package's normal clean-tree guard.
  run scripts/check-quepaxa-package.sh --allow-dirty
fi
